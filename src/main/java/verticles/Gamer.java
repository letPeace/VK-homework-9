package verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Verticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.impl.JavaVerticleFactory;
import utils.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public class Gamer extends AbstractVerticle {

    private final String name;
    private String clanName;
    private long timerId;
    private MessageConsumer exit;
    private MessageConsumer gamers;

    public Gamer(long number) {
        this.name = "gamer#" + number;
    }

    @Override
    public void start() {
        joinClanPeriodically();
    }

    private void joinClan(String clanToJoin) {
        System.out.println(name + " tries to join "+ clanToJoin);
        vertx.eventBus().request(clanToJoin + Values.gamers, name, new DeliveryOptions(), reply -> {
            if (reply.succeeded()) {
                clanName = clanToJoin;

                exit();
                listenToGamers();
                pingGamersPeriodically();

                // добавляем свое имя в мапу gamers
                vertx.sharedData().getAsyncMap(clanName + Values.gamers, map -> {
                    map.result().put(name, clanName, done -> {
                        System.out.println(name + " puts name to "+ clanName + Values.gamers);
                    });
                });
            } else{
                if (ThreadLocalRandom.current().nextBoolean()) { // [если заявка отклонена, то с некоторой вероятностью повторяет ее через заданный промежуток времени]
                    vertx.setTimer(2000, event -> joinClan(clanToJoin));
                }
            }
        });
    }

    private void joinClanPeriodically() {
        // [получает список активных кланов и отправляет заявку на вступление в один из них]
        pingPeriodically(Values.clans, true);
    }

    private void pingGamersPeriodically() {
        // если заявка принята, то [с некоторой периодичностью получает список членов клана и посылает сообщение случайно выбранному из них]
        pingPeriodically(clanName+Values.gamers, false);
    }

    private void pingPeriodically(String mapName, boolean toClan){
        vertx.cancelTimer(timerId);
        timerId = vertx.setPeriodic(5000, timer -> {
            vertx.sharedData().getAsyncMap(mapName, map -> {
                map.result().size().onComplete(size -> {
                    if(size.result() > 0) {
                        map.result().keys().onComplete(set -> {
                            vertx.executeBlocking(handler -> {
                                int random = Math.abs(ThreadLocalRandom.current().nextInt() % size.result());
                                List<Object> list = new ArrayList<>(set.result());
                                if(toClan){ // join clan
                                    joinClan(list.get(random).toString());
                                } else{ // ping gamer
                                    vertx.eventBus().send(list.get(random).toString(), name);
                                    System.out.println(name+" messages to "+list.get(random).toString());
                                }
                            });
                        });
                    }
                });
            });
        });
    }

    private void listenToGamers(){
        if(gamers != null) gamers.unregister();
        gamers = vertx.eventBus().consumer(clanName+Values.gamers, event -> {
            System.out.println(name+" got message from "+event.body());
        });
    }

    private void exit(){
        // comes from Admin
        if(exit != null) exit.unregister();
        exit = vertx.eventBus().consumer(clanName + Values.exit, event -> {
            clanName = null;
            joinClanPeriodically();
        });
    }

    public static final class Factory extends JavaVerticleFactory {
        private long number;

        public Factory() {
            this(0);
        }

        public Factory(int startNumber) {
            number = startNumber;
        }

        @Override
        public String prefix() {
            return Values.prefix;
        }

        @Override
        public void createVerticle(String verticleName,
                                   ClassLoader classLoader,
                                   Promise<Callable<Verticle>> promise) {
            promise.complete(() -> new Gamer(number++));
        }
    }

}
