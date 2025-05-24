package org.unict.pds.actor.server;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import lombok.RequiredArgsConstructor;
import org.unict.pds.message.publish.PublishManagerResponse;
import org.unict.pds.message.publish.PublishMessageRelease;
import org.unict.pds.message.subscribe.SubscribeTopicResponse;

import java.util.List;

@RequiredArgsConstructor
public class InternalHandler {
    private final MQTTManager actor;

    public void handleSubscriptionResponse(SubscribeTopicResponse subscribeTopicResponse) {
        int packetId = subscribeTopicResponse.messageId();
        List<MqttReasonCodes.SubAck> responseCodes = subscribeTopicResponse.responseCodes();
        int[] qosLevels = responseCodes.stream()
                .mapToInt(MqttReasonCodes.SubAck::byteValue).toArray();
        actor.getProtocolHandler().sendSubAck(packetId, qosLevels);
    }

    public void forwardSubscribeToTopicManager(MqttSubscribeMessage message) {
        System.out.println("Forwarding SUBSCRIBE message to SubscriptionManager");
        actor.getSubscriptionManager().tell(message, actor.getSelf());
    }

    public void forwardPublishToPublishManager(MqttPublishMessage message) {
        System.out.println("Forwarding PUBLISH message to PublishingManager");
        actor.getPublishManager().tell(message, actor.getSelf());
    }

    public void handlePublishManagerResponse(PublishManagerResponse response) {
        actor.getProtocolHandler().sendPubAck(response.messageId());
    }

    public void handlePublishMessageRelease(PublishMessageRelease message) {
        actor.getProtocolHandler().sendMqttMessage(message.message());
    }
}