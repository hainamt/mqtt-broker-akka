package org.unict.pds.actor.server;


import akka.io.TcpMessage;
import akka.util.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import lombok.RequiredArgsConstructor;
import org.unict.pds.message.publish.PublishMessage;
import org.unict.pds.message.subscribe.SubscribeMessage;
import org.unict.pds.message.subscribe.UnsubscribeMessage;

import java.util.List;

@RequiredArgsConstructor
public class ProtocolHandler {
    private final MQTTManager actor;
    private final EmbeddedChannel encodeChannel = new EmbeddedChannel(MqttEncoder.INSTANCE);

    public void processProtocolMessage(MqttMessage message) {
        switch (message.fixedHeader().messageType()) {
            case SUBSCRIBE:
                System.out.println("Handling SUBSCRIBE message");
                onReceiveInBoundSubscribe((MqttSubscribeMessage) message);
                break;

            case UNSUBSCRIBE:
                System.out.println("Handling UNSUBSCRIBE message");
                onReceiveInboundUnsubscribe((MqttUnsubscribeMessage) message);
                break;

            case PUBLISH:
                System.out.println("Handling PUBLISH message");
                onReceiveInboundPublish((MqttPublishMessage) message);
                break;

            case CONNECT:
                System.out.println("Handling CONNECT message");
                System.out.println("Client: " + actor.getSender().path().address().toString());
                onReceiveInboundConnect();
                break;

            case DISCONNECT:
                System.out.println("Handling DISCONNECT message");
                onReceiveInboundDisconnect();
                break;

            case PINGREQ:
                System.out.println("Handling PING request");
                onReceiveInboundPing();
                break;

            default:
                System.out.println("Received unhandled message type: " + message.fixedHeader().messageType());
                break;
        }
    }


    private void onReceiveInboundConnect() {
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

    private void onReceiveInboundDisconnect() {
        System.out.println("Client disconnected");
        actor.getTcpConnection().tell(TcpMessage.close(), actor.getSelf());
    }

    private void onReceiveInboundPing() {
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

    private void onReceiveInBoundSubscribe(MqttSubscribeMessage message) {
        actor.getInternalHandler().onReceiveSubscribeRequest(new SubscribeMessage.Request(message));
    }

    public void onReceiveOutboundSubAck(SubscribeMessage.Response response) {
        int packetId = response.messageId();
        List<MqttReasonCodes.SubAck> responseCodes = response.responseCodes();
        int[] qosLevels = responseCodes.stream()
                .mapToInt(MqttReasonCodes.SubAck::byteValue).toArray();

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

    public void onReceiveInboundUnsubscribe(MqttUnsubscribeMessage message) {
        actor.getInternalHandler().onReceiveUnsubscribeRequest(new UnsubscribeMessage.Request(message));
    }

    public void onReceiveOutboundUnsubAck(UnsubscribeMessage.Response response) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.UNSUBACK,
                false,
                MqttQoS.AT_MOST_ONCE,
                false,
                0);

        MqttMessageIdVariableHeader variableHeader =
                MqttMessageIdVariableHeader.from(response.messageId());
        short[] reasonCodes = new short[response.reasonCodes().size()];
        for (int i = 0; i < response.reasonCodes().size(); i++) {
            reasonCodes[i] = response.reasonCodes().get(i).byteValue();
        }

        MqttUnsubAckMessage unsubAckMessage = new MqttUnsubAckMessage(
                fixedHeader,
                variableHeader,
                new MqttUnsubAckPayload(reasonCodes));
        sendMqttMessage(unsubAckMessage);

    }

    public void onReceiveInboundPublish(MqttPublishMessage message) {
        System.out.println("Processing PUBLISH message to topic: " + message.variableHeader().topicName());
        actor.getInternalHandler().onReceivePublishRequest(new PublishMessage.Request(message));
    }

    public void onReceiveOutboundPublish(PublishMessage.Release message) {
        System.out.println("Processing PUBLISH message release to topic: " + message.message().variableHeader().topicName());
        sendMqttMessage(message.message());
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