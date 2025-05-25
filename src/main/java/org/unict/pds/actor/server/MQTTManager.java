package org.unict.pds.actor.server;

import akka.actor.*;
import akka.io.Tcp;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.unict.pds.actor.ActorResolutionUtils;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.MQTTManagerConfiguration;
import org.unict.pds.exception.CriticalActorCouldNotBeResolved;
import org.unict.pds.logging.LoggingUtils;
import org.unict.pds.message.publish.PublishMessage;
import org.unict.pds.message.subscribe.SubscribeMessage;
import org.unict.pds.message.subscribe.UnsubscribeMessage;

@Getter
@Setter
@RequiredArgsConstructor
public class MQTTManager extends AbstractActor {
    private final EmbeddedChannel decodeChannel = new EmbeddedChannel(new MqttDecoder(65536));
    private ActorRef subscriptionManager;
    private ActorRef publishManager;

    private final ActorRef tcpConnection;
    private final ProtocolHandler protocolHandler = new ProtocolHandler(this);
    private final InternalHandler internalHandler = new InternalHandler(this);

    
    @Override
    public void preStart() {

        MQTTManagerConfiguration configuration = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).mqttManagerConfig();

        try {
            this.subscriptionManager = ActorResolutionUtils.resolveActor(
                    getContext().getSystem(),
                    configuration.subscriptionManagerAddress(),
                    configuration.resolutionTimeout(),
                    "MQTTManager",
                    true
            );

            this.publishManager = ActorResolutionUtils.resolveActor(
                    getContext().getSystem(),
                    configuration.publishManagerAddress(),
                    configuration.resolutionTimeout(),
                    "MQTTManager",
                    true
            );
            System.out.println("Successfully resolved Publish manager");
        } catch (CriticalActorCouldNotBeResolved e) {
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.ERROR,
                    "MQTTManager startup failed: " + e.getMessage(),
                    "MQTTManager"
            );
            throw e;
        }

        LoggingUtils.logApplicationEvent(
                LoggingUtils.LogLevel.INFO,
                "MQTT Manager started, successfully resolved PublishManager",
                "MQTTManager"
        );
        LoggingUtils.logApplicationEvent(
                LoggingUtils.LogLevel.INFO,
                "MQTT Manager started, successfully resolved SubscriptionManager",
                "MQTTManager"
        );
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Tcp.Received.class, this::handleTcpMessage)
                .match(Tcp.ConnectionClosed.class, this::handleConnectionClosed)
                .match(SubscribeMessage.Response.class, internalHandler::onReceiveSubscriptionResponse)
                .match(UnsubscribeMessage.Response.class, internalHandler::onReceiveUnsubscribeResponse)
                .match(PublishMessage.Release.class, internalHandler::onReceivePublishMessageRelease)
                .build();
    }
    
    private void handleTcpMessage(Tcp.Received msg) {
        final byte[] data = msg.data().toArray();
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        decodeChannel.writeInbound(buf);
        MqttMessage message = decodeChannel.readInbound();
        protocolHandler.processProtocolMessage(message);
    }
    
    private void handleConnectionClosed(Tcp.ConnectionClosed msg) {
        decodeChannel.close();
        getContext().stop(getSelf());
    }

    public ActorRef getSender() {
        return super.getSender();
    }
    
    public ActorRef getSelf() {
        return super.getSelf();
    }

}