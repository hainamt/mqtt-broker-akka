package org.unict.pds.message.publish;

import io.netty.handler.codec.mqtt.MqttPublishMessage;

public record PublishMessageRelease(
        MqttPublishMessage message
) {
}
