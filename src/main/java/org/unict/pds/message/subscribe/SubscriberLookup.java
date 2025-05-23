package org.unict.pds.message.subscribe;

import akka.actor.ActorRef;

import java.util.List;

public record SubscriberLookup() {
    public record Request(
            String topic
    ) {}

    public record Response(
            String topic,
            List<ActorRef> subscribers
    ) {}
}
