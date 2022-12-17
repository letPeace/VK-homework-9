package verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Verticle;
import io.vertx.core.impl.JavaVerticleFactory;
import utils.Values;

import java.util.concurrent.Callable;

public class Admin extends AbstractVerticle {

    private final String clanName;
    private final String name;
    private final long moderatorsQuantity;
    private final long gamersQuantity;
    private boolean state; // online or offline

    public Admin(long number, long moderatorsQuantity, long gamersQuantity){
        this.clanName = "clan#" + number;
        this.name = "admin#" + number;
        this.moderatorsQuantity = moderatorsQuantity;
        this.gamersQuantity = gamersQuantity;
        this.state = true;
    }

    public Admin(long number){
        this(number, number+2, number+5);
    }

    @Override
    public void start() {
        // добавляем свое имя в мапу gamers
        vertx.sharedData().getAsyncMap(clanName + Values.gamers, map -> {
            map.result().put(name, clanName, done -> {
                System.out.println(name + " puts name to "+ clanName + Values.gamers+" (admin)");
            });
        });

        // timer 10sec: go to offline and back
        vertx.setPeriodic(10000, timer -> {
            state = !state;
            setState(state);
        });

        // [если текущее количество превышает лимит, рассылает всем участникам сообщение о необходимости выйти и снова подать заявку на вступление]
        vertx.setPeriodic(5000, timer -> {
            if(state){
                checkQuantity(clanName + Values.gamers, gamersQuantity);
                checkQuantity(clanName + Values.moderators, moderatorsQuantity);
            }
        });

        // listen to moderators
        vertx.eventBus().consumer(clanName + Values.moderators, event -> {
            vertx.sharedData().getCounter(clanName + Values.moderators, counter -> {
                if(counter.succeeded()) {
                    counter.result().get(quantity -> {
                        if(quantity.result() < moderatorsQuantity) {
                            System.out.println(event.body()+" is applied as Moderator №("+(quantity.result()+1)+") in "+clanName);
                            event.reply(gamersQuantity);
                            counter.result().getAndIncrement();
                        }
                    });
                }
            });
        });
    }

    private void setState(boolean stateToSet){
        vertx.sharedData().getAsyncMap(Values.clans, map -> {
            map.result().put(clanName, stateToSet);
        });
    }

    private void checkQuantity(String mapName, long correctQuantity){
        vertx.sharedData().getCounter(mapName, counter -> {
            if(counter.succeeded()) {
                counter.result().get(quantity -> {
                    if(quantity.result() > correctQuantity) {
                        System.out.println(clanName + " ("+mapName+") -> exit due to "+quantity.result()+" > "+correctQuantity);
                        vertx.eventBus().publish(clanName + Values.exit, Values.exit);
                    } else{
                        System.out.println(clanName + " ("+mapName+") has correct quantity = "+quantity.result()+" of "+correctQuantity);
                    }
                });
            }
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
            promise.complete(() -> new Admin(number++));
        }
    }

}
