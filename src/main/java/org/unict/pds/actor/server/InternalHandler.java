package org.unict.pds.actor.server;

import lombok.RequiredArgsConstructor;
import org.unict.pds.message.publish.PublishMessage;
import org.unict.pds.message.subscribe.SubscribeMessage;
import org.unict.pds.message.subscribe.UnsubscribeMessage;

@RequiredArgsConstructor
public class InternalHandler {
    private final MQTTManager actor;

    public void onReceiveSubscriptionResponse(SubscribeMessage.Response response) {
        actor.getProtocolHandler().onReceiveOutboundSubAck(response);
    }

    public void onReceiveSubscribeRequest(SubscribeMessage.Request message) {
        System.out.println("Forwarding SUBSCRIBE message to SubscriptionManager");
        actor.getSubscriptionManager().tell(message, actor.getSelf());
    }

    public void onReceiveUnsubscribeRequest(UnsubscribeMessage.Request message) {
        System.out.println("Forwarding UNSUBSCRIBE message to SubscriptionManager");
        actor.getSubscriptionManager().tell(message, actor.getSelf());
    }

    public void onReceiveUnsubscribeResponse(UnsubscribeMessage.Response message) {
        actor.getProtocolHandler().onReceiveOutboundUnsubAck(message);
    }

    public void onReceivePublishRequest(PublishMessage.Request message) {
        System.out.println("Forwarding PUBLISH message to PublishingManager");
        actor.getPublishManager().tell(message, actor.getSelf());
    }

    public void onReceivePublishMessageRelease(PublishMessage.Release release) {
        actor.getProtocolHandler().onReceiveOutboundPublish(release);
    }

}