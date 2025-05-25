package org.unict.pds;

import akka.actor.ActorSystem;
import akka.actor.Props;
import org.unict.pds.actor.security.Authenticator;
import org.unict.pds.actor.server.TCPMQTTServer;
import org.unict.pds.actor.publishing.PublishManager;
import org.unict.pds.actor.subscription.SubscriptionManager;
import org.unict.pds.actor.topic.TopicManager;

public class Main {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("mqtt-broker");

        system.actorOf(
                Props.create(Authenticator.class),
                "authenticator");

        system.actorOf(
                Props.create(TopicManager.class),
                "topic-manager");

        system.actorOf(
                Props.create(SubscriptionManager.class),
                "subscription-manager");

        system.actorOf(
                Props.create(PublishManager.class),
                "publish-manager");

        system.actorOf(
                Props.create(TCPMQTTServer.class),
                "tcpmqtt-server");
    }
}