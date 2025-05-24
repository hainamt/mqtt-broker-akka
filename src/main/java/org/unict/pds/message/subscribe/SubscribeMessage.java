package org.unict.pds.message.subscribe;

import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;

import java.util.List;

public record SubscribeMessage() {

    public record Request(
            MqttSubscribeMessage message
    ) {}

    public record Response(
            int messageId,
            List<MqttReasonCodes.SubAck> responseCodes
    ) {}
}
