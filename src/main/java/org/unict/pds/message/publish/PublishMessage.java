package org.unict.pds.message.publish;

import io.netty.handler.codec.mqtt.MqttPublishMessage;

public record PublishMessage() {
    public record Request(
            MqttPublishMessage message
    ) {}

    public record Release(
            MqttPublishMessage message
    ) {}
}
