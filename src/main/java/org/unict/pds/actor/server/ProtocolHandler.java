package org.unict.pds.actor.server;


import akka.io.TcpMessage;
import akka.util.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import lombok.RequiredArgsConstructor;
import org.unict.pds.message.publish.PublishMessageRelease;

@RequiredArgsConstructor
public class ProtocolHandler {
    private final MQTTManager actor;
    private final EmbeddedChannel encodeChannel = new EmbeddedChannel(MqttEncoder.INSTANCE);

    public void processProtocolMessage(MqttMessage message) {
        switch (message.fixedHeader().messageType()) {
            case SUBSCRIBE:
                System.out.println("Handling SUBSCRIBE message");
                handleSubscribe((MqttSubscribeMessage) message);
                break;

            case PUBLISH:
                System.out.println("Handling PUBLISH message");
                handlePublishRequest((MqttPublishMessage) message);
                break;

            case CONNECT:
                System.out.println("Handling CONNECT message");
                System.out.println("Client: " + actor.getSender().path().address().toString());
                handleConnect();
                break;

            case DISCONNECT:
                System.out.println("Handling DISCONNECT message");
                handleDisconnect();
                break;

            case PINGREQ:
                System.out.println("Handling PING request");
                handlePingRequest();
                break;

            default:
                System.out.println("Received unhandled message type: " + message.fixedHeader().messageType());
                break;
        }
    }


    private void handleConnect() {
        System.out.println("Processing CONNECT message");

        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.CONNACK,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                2);

        MqttConnAckVariableHeader variableHeader = new MqttConnAckVariableHeader(
                MqttConnectReturnCode.CONNECTION_ACCEPTED,
                false);

        MqttConnAckMessage connAckMessage = new MqttConnAckMessage(fixedHeader, variableHeader);
        System.out.println("Sending CONNACK message");
        sendMqttMessage(connAckMessage);
    }

    private void handleDisconnect() {
        System.out.println("Client disconnected");
        actor.getTcpConnection().tell(TcpMessage.close(), actor.getSelf());
    }

    private void handlePingRequest() {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PINGRESP,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0);

        MqttMessage pingRespMessage = new MqttMessage(fixedHeader);
        System.out.println("Sending PINGRESP message");
        sendMqttMessage(pingRespMessage);
    }

    private void handleSubscribe(MqttSubscribeMessage message) {
        actor.getInternalHandler().forwardSubscribeToTopicManager(message);
    }

    public void sendSubAck(int packetId, int[] qosLevels) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.SUBACK,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(packetId);
        MqttSubAckPayload payload = new MqttSubAckPayload(qosLevels);
        MqttSubAckMessage subAckMessage = new MqttSubAckMessage(fixedHeader, variableHeader, payload);

        System.out.println("Sending SUBACK message");
        sendMqttMessage(subAckMessage);
    }

    private void handlePublishRequest(MqttPublishMessage message) {
        System.out.println("Processing PUBLISH message to topic: " + message.variableHeader().topicName());
        actor.getInternalHandler().forwardPublishToPublishManager(message);
    }

    private void handlePublishRelease(PublishMessageRelease message) {
        System.out.println("Processing PUBLISH message release to topic: " + message.message().variableHeader().topicName());
        sendMqttMessage(message.message());
    }

    public void sendPubAck(int packetId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBACK,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                2);

        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(packetId);

        MqttPubAckMessage pubAckMessage = new MqttPubAckMessage(fixedHeader, variableHeader);
        System.out.println("Sending PUBACK message");
        sendMqttMessage(pubAckMessage);
    }

    public void sendMqttMessage(MqttMessage message) {
        encodeChannel.writeOutbound(message);
        ByteBuf msgBuf = encodeChannel.readOutbound();
        if (msgBuf != null) {
            actor.getTcpConnection().tell(TcpMessage.write(ByteString.fromByteBuffer(msgBuf.nioBuffer())),
                    actor.getSelf());
        } else {
            System.err.println("Failed to encode MQTT message");
        }
    }

}