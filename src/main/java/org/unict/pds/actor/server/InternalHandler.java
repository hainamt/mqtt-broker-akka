package org.unict.pds.actor.mqtt;

import akka.io.TcpMessage;
import akka.util.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import lombok.RequiredArgsConstructor;
import org.unict.pds.message.topic.SubscriptionManagerResponse;

import java.util.List;

@RequiredArgsConstructor
public class InternalHandler {
    private final MQTTHandler actor;

    public void handleSubscriptionResponse(SubscriptionManagerResponse subscriptionManagerResponse) {
        int packetId = subscriptionManagerResponse.messageId();
        List<MqttReasonCodes.SubAck> responseCodes = subscriptionManagerResponse.responseCodes();
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

        actor.getEncodeChannel().writeOutbound(subAckMessage);
        ByteBuf msgBuf = actor.getEncodeChannel().readOutbound();
        actor.getSender().tell(TcpMessage.write(ByteString.fromByteBuffer(msgBuf.nioBuffer())), actor.getSelf());
    }

    // Add other internal response handlers as needed
}