package org.unict.pds.message.publish;

import akka.actor.ActorRef;
import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.util.List;

public record PublishMessage() {
    public record Request(
            MqttPublishMessage message
    ) {}

    public record RequestWithSubscribers(
            MqttPublishMessage message,
            List<ActorRef> subscribers
    ) {}

    public record Release(
            MqttPublishMessage message
    ) {}
}
