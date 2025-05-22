package org.unict.pds.message.subscribe;

import io.netty.handler.codec.mqtt.MqttReasonCodes;

import java.util.List;

public record SubscribeTopicResponse(
        int messageId,
        List<MqttReasonCodes.SubAck> responseCodes
) { }
