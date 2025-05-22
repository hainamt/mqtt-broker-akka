package org.unict.pds;

import akka.actor.ActorSystem;
import akka.actor.Props;
import org.unict.pds.actor.TCPMQTTServer;
import org.unict.pds.actor.subscription.SubscriptionManager;
import org.unict.pds.actor.TopicManager;

public class Main {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("mqtt-broker");

        system.actorOf(
                Props.create(TopicManager.class, true),
                "topic-manager");

        system.actorOf(
                Props.create(SubscriptionManager.class),
                "subscription-manager");

        system.actorOf(
                Props.create(TCPMQTTServer.class),
                "tcpmqtt-server");

    }
}