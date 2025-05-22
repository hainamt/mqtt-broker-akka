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
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.MQTTManagerConfiguration;
import org.unict.pds.message.subscribe.SubscribeTopicResponse;

@Getter
@Setter
@RequiredArgsConstructor
public class MQTTManager extends AbstractActor {
    private final EmbeddedChannel decodeChannel = new EmbeddedChannel(new MqttDecoder(65536));
    private ActorRef subscriptionManager;
    private ActorRef publishManager;
    
    private final ProtocolHandler protocolHandler = new ProtocolHandler(this);
    private final InternalHandler internalHandler = new InternalHandler(this);

    
    @Override
    public void preStart() {

        MQTTManagerConfiguration configuration = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).mqttManagerConfig();

        ActorSelection subscriptionManagerSelection = getContext()
                .actorSelection(configuration.subscriptionManagerAddress());
        subscriptionManagerSelection.resolveOne(java.time.Duration.ofSeconds(3))
                .whenComplete((actorRef, throwable) -> {
                    if (throwable != null) {
                        System.err.println("Could not resolve subscription manager: " + throwable.getMessage());
                    } else {
                        this.subscriptionManager = actorRef;
                        System.out.println("Successfully resolved subscription manager");
                    }
                });

        ActorSelection publishManagerSelection = getContext()
                .actorSelection(configuration.publishManagerAddress());
        publishManagerSelection.resolveOne(java.time.Duration.ofSeconds(3))
                .whenComplete((actorRef, throwable) -> {
                    if (throwable != null) {
                        System.err.println("Could not resolve Publish manager: " + throwable.getMessage());
                    } else {
                        this.publishManager = actorRef;
                        System.out.println("Successfully resolved Publish manager");
                    }
                });
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Tcp.Received.class, this::handleTcpMessage)
                .match(Tcp.ConnectionClosed.class, this::handleConnectionClosed)
                .match(SubscribeTopicResponse.class, internalHandler::handleSubscriptionResponse)

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