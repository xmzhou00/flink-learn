package com.akkaremote;

import akka.actor.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class LocalActor extends UntypedAbstractActor {
    private ActorSelection remoteActor;


    @Override
    public void preStart() throws Exception, Exception {
        remoteActor = context().actorSelection("akka.tcp://RemoteSystem@localhost:7568/user/remote");
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof String) {
            System.out.println("Local received message: " + message);
            // 发送消息给远程Actor
            remoteActor.tell(message+" I'm local machine !", getSelf());
        }
    }

    public static void main(String[] args) {

        ActorSystem system = ActorSystem.create("localSystem",ConfigFactory.parseString("akka {\n" +
                "  actor {\n" +
                "    provider = \"akka.remote.RemoteActorRefProvider\"\n" +
                "  }\n" +
                "  remote {\n" +
                "    enabled-transports = [\"akka.remote.netty.tcp\"]\n" +
                "    netty.tcp {\n" +
                "      hostname = localhost\n" +
                "      port = 8888\n" +
                "    }\n" +
                "    log-sent-messages = on\n" +
                "    log-received-messages = on\n" +
                "  }\n" +
                "}"));
        ActorRef local = system.actorOf(Props.create(LocalActor.class), "local");

        local.tell("hello", ActorRef.noSender());
    }
}
