package org.unict.pds.actor.mqtt;


import akka.io.TcpMessage;
import akka.util.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ProtocolHandler {
    private final MQTTHandler actor;

    public void processProtocolMessage(MqttMessage message) {
        switch (message.fixedHeader().messageType()) {
            case SUBSCRIBE:
                System.out.println("Handling SUBSCRIBE message");
                handleSubscribe((MqttSubscribeMessage) message);
                break;

            case PUBLISH:
                System.out.println("Handling PUBLISH message");
                handlePublish((MqttPublishMessage) message);
                break;

            case CONNECT:
                System.out.println("Handling CONNECT message");
                handleConnect();
                break;

            case DISCONNECT:
                System.out.println("Handling DISCONNECT message");
                handleDisconnect(message);
                break;

            case PINGREQ:
                System.out.println("Handling PING request");
                handlePingRequest(message);
                break;

            default:
                System.out.println("Received unhandled message type: " + message.fixedHeader().messageType());
                break;
        }
    }

    private void handleSubscribe(MqttSubscribeMessage message) {
        if (actor.getSubscriptionManager() != null) {
            actor.getSubscriptionManager().tell(message, actor.getSelf());
        } else {
            System.err.println("Subscription Manager not available");
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

    private void handleDisconnect(MqttMessage message) {
        System.out.println("Client disconnected");
    }

    private void handlePingRequest(MqttMessage message) {
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

    private void handlePublish(MqttPublishMessage message) {
        System.out.println("Processing PUBLISH message to topic: " + message.variableHeader().topicName());
    }

    private void sendMqttMessage(MqttMessage message) {
        actor.getEncodeChannel().writeOutbound(message);
        ByteBuf msgBuf = actor.getEncodeChannel().readOutbound();
        if (msgBuf != null) {
            actor.getSender().tell(TcpMessage.write(ByteString.fromByteBuffer(msgBuf.nioBuffer())), actor.getSelf());
        } else {
            System.err.println("Failed to encode MQTT message");
        }
    }
}