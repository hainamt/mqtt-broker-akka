package org.unict.pds.actor.server;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.unict.pds.logging.LoggingUtils;
import org.unict.pds.message.publish.PublishMessage;
import org.unict.pds.message.subscribe.SubscribeMessage;
import org.unict.pds.message.subscribe.UnsubscribeMessage;

@Slf4j
@RequiredArgsConstructor
public class InternalHandler {
    private final MQTTManager actor;

    public void onReceiveSubscribeRequest(SubscribeMessage.Request message) {
        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                SubscribeMessage.Request.class,
                actor.getSender().path().toString(),
                actor.getSelf().path().toString(),
                "InternalHandler.SubscriberManager"
        );
        actor.getSubscriptionManager().tell(message, actor.getSelf());
    }

    public void onReceiveSubscriptionResponse(SubscribeMessage.Response response) {
        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                SubscribeMessage.Response.class,
                actor.getSender().path().toString(),
                actor.getSelf().path().toString(),
                "SubscriberManager.InternalHandler"
        );
        actor.getProtocolHandler().onReceiveOutboundSubAck(response);
    }

    public void onReceiveUnsubscribeRequest(UnsubscribeMessage.Request message) {
        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                UnsubscribeMessage.Request.class,
                actor.getSelf().path().toString(),
                actor.getSubscriptionManager().path().toString(),
                "InternalHandler.SubscriberManager"
        );
        actor.getSubscriptionManager().tell(message, actor.getSelf());
    }

    public void onReceiveUnsubscribeResponse(UnsubscribeMessage.Response message) {
        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                UnsubscribeMessage.Response.class,
                actor.getSelf().path().toString(),
                actor.getSubscriptionManager().path().toString(),
                "SubscriberManager.InternalHandler"
        );
        actor.getProtocolHandler().onReceiveOutboundUnsubAck(message);
    }

    public void onReceivePublishRequest(PublishMessage.Request message) {
        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                PublishMessage.Request.class,
                actor.getSelf().path().toString(),
                actor.getPublishManager().path().toString(),
                "InternalHandler.PublishManager"
        );
        actor.getPublishManager().tell(message, actor.getSelf());
    }

    public void onReceivePublishMessageRelease(PublishMessage.Release release) {
        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                PublishMessage.Release.class,
                actor.getSelf().path().toString(),
                actor.getPublishManager().path().toString(),
                "PublishManager.InternalHandler"
        );
        actor.getProtocolHandler().onReceiveOutboundPublish(release);
    }

}