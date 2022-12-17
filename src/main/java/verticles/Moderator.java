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

public class Moderator extends AbstractVerticle {

    private final String name;
    private String clanName;
    private long gamersQuantity;
    private long timerId;
    private MessageConsumer exit;
    private MessageConsumer gamers;

    public Moderator(long number) {
        this.name = "moderator#" + number;
    }

    @Override
    public void start() {
        joinClanPeriodically();
    }

    private void joinClan(String clanToJoin) {
        System.out.println(name + " tries to join "+ clanToJoin+" (moderator)");
        vertx.eventBus().request(clanToJoin + Values.moderators, name, new DeliveryOptions(), reply -> {
            if (reply.succeeded()) {
                clanName = clanToJoin;
                gamersQuantity = (long) reply.result().body();
                System.out.println(name + " was accepted in " + clanToJoin+" with gamers quantity = "+gamersQuantity);

                exit();
                listenToGamers();

                // добавляем свое имя в мапу gamers
                vertx.sharedData().getAsyncMap(clanName + Values.gamers, map -> {
                    map.result().put(name, clanName, done -> {
                        System.out.println(name + " puts name to "+ clanName + Values.gamers+" (moderator)");
                    });
                });
            } else{
                if (ThreadLocalRandom.current().nextBoolean()) { // [если заявка отклонена, то с некоторой вероятностью повторяет ее через заданный промежуток времени]
                    vertx.setTimer(2000, timer -> joinClan(clanToJoin));
                }
            }
        });
    }

    private void joinClanPeriodically() {
        vertx.cancelTimer(timerId);
        timerId = vertx.setPeriodic(5000, timer -> {
            vertx.sharedData().getAsyncMap(Values.clans, map -> {
                map.result().size().onComplete(size -> {
                    if(size.result() > 0) {
                        map.result().keys().onComplete(set -> {
                            vertx.executeBlocking(handler -> {
                                int random = Math.abs(ThreadLocalRandom.current().nextInt() % size.result());
                                List<Object> list = new ArrayList<>(set.result());
                                joinClan(list.get(random).toString());
                            });
                        });
                    }
                });
            });
        });
    }

    private void listenToGamers(){
        if(gamers != null) gamers.unregister();
        gamers = vertx.eventBus().consumer(clanName + Values.gamers, event -> { // [принимает заявки от рядовых пользователей]
            vertx.sharedData().getCounter(clanName + Values.gamers, counter -> {
                if (counter.succeeded()) {
                    counter.result().get(quantity -> {
                        if (quantity.result() < gamersQuantity) { // [одобряет заявку в состоянии ожидания, если лимит на количество участников не превышен]
                            System.out.println(event.body()+" is applied as Gamer №("+(quantity.result()+1)+") in "+clanName);
                            event.reply(true);
                            counter.result().getAndIncrement();
                        }
                    });
                }
            });
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
            promise.complete(() -> new Moderator(number++));
        }
    }

}
