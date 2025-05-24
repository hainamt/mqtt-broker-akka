package org.unict.pds.message.subscribe;

import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;

import java.util.List;

public record UnsubscribeMessage(
) {
    public record Request(
            MqttUnsubscribeMessage message
    ) {}

    public record Response(
            int messageId,
            List<MqttReasonCodes.UnsubAck> reasonCodes
    ) {}
}
