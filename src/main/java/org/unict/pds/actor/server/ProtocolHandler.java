package org.unict.pds.actor.server;


import akka.io.TcpMessage;
import akka.util.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.unict.pds.logging.LoggingUtils;
import org.unict.pds.message.publish.PublishMessage;
import org.unict.pds.message.subscribe.SubscribeMessage;
import org.unict.pds.message.subscribe.UnsubscribeMessage;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class ProtocolHandler {
    private final MQTTManager actor;
    private final EmbeddedChannel encodeChannel = new EmbeddedChannel(MqttEncoder.INSTANCE);

    public void processProtocolMessage(MqttMessage message) {
        LoggingUtils.logProtocolMessage(
                LoggingUtils.LogLevel.INFO,
                message.fixedHeader().messageType(),
                actor.getTcpConnection().path().address().toString(),
                actor.getSelf().path().address().toString(),
                true
        );

        switch (message.fixedHeader().messageType()) {
            case SUBSCRIBE:
                onReceiveInBoundSubscribe((MqttSubscribeMessage) message);
                break;

            case UNSUBSCRIBE:
                onReceiveInboundUnsubscribe((MqttUnsubscribeMessage) message);
                break;

            case PUBLISH:
                onReceiveInboundPublish((MqttPublishMessage) message);
                break;

            case CONNECT:
                onReceiveInboundConnect();
                break;

            case DISCONNECT:
                onReceiveInboundDisconnect();
                break;

            case PINGREQ:
                onReceiveInboundPing();
                break;

            default:
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
        sendMqttMessage(connAckMessage);
    }

    private void onReceiveInboundDisconnect() {
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
        actor.getInternalHandler().onReceivePublishRequest(new PublishMessage.Request(message));
    }

    public void onReceiveOutboundPublish(PublishMessage.Release message) {
        sendMqttMessage(message.message());
    }

    public void sendMqttMessage(MqttMessage message) {
        LoggingUtils.logProtocolMessage(
                LoggingUtils.LogLevel.INFO,
                message.fixedHeader().messageType(),
                actor.getSelf().path().address().toString(),
                actor.getSender().path().address().toString(),
                false
        );

        encodeChannel.writeOutbound(message);
        ByteBuf msgBuf = encodeChannel.readOutbound();
        if (msgBuf != null) {
            actor.getTcpConnection().tell(TcpMessage.write(ByteString.fromByteBuffer(msgBuf.nioBuffer())),
                    actor.getSelf());
        } else {
            LoggingUtils.logApplicationEvent(LoggingUtils.LogLevel.ERROR,
                    "Failed to encode MQTT Message",
                    actor.getClass().getSimpleName());
        }
    }


}