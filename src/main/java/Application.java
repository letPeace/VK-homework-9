import io.vertx.core.*;
import verticles.Admin;
import verticles.Gamer;
import verticles.Moderator;

public class Application {

    public static void main(String[] args){
        Vertx.clusteredVertx(
            new VertxOptions(),
            vertxResult -> {
                final var vertx = vertxResult.result();
                for(int i=0; i<20; i++){
                    vertx.deployVerticle(new Gamer(i), new DeploymentOptions().setWorker(true));
                    if(i<10) vertx.deployVerticle(new Moderator(i), new DeploymentOptions().setWorker(true));
                    if(i<5) vertx.deployVerticle(new Admin(i), new DeploymentOptions().setWorker(true));
                }
            }
        );
    }

}
