package com.akkaremote;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import com.typesafe.config.ConfigFactory;

public class RemoteActor extends UntypedAbstractActor {
    @Override
    public void onReceive(Object message) throws Throwable, Throwable {
        if (message instanceof String) {
            System.out.println("receive msg: " + message);
        }
    }

    public static void main(String[] args) {
        // 创建远程ActorSystem
        ActorSystem remoteSystem = ActorSystem.create("RemoteSystem", ConfigFactory.parseString("" +
                "akka {\n" +
                "  actor {\n" +
                "    provider = \"akka.remote.RemoteActorRefProvider\"\n" +
                "  }\n" +
                "  remote {\n" +
                "    enabled-transports = [\"akka.remote.netty.tcp\"]\n" +
                "    netty.tcp {\n" +
                "      hostname = localhost\n" +
                "      port = 7568\n" +
                "    }\n" +
                "    log-sent-messages = on\n" +
                "    log-received-messages = on\n" +
                "  }\n" +
                "}"));

        // 创建远程Actor
        ActorRef remoteActor = remoteSystem.actorOf(Props.create(RemoteActor.class), "remote");
        remoteActor.tell("the RemoteActor is alive", ActorRef.noSender());
    }
}
