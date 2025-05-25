package org.unict.pds.message.security;

import io.netty.handler.codec.mqtt.MqttConnectMessage;

import java.util.Set;

public record ConnectWithAuthentication() {
    public record Request(MqttConnectMessage message) {}

    public record Response(
            boolean authenticated,
            String username,
            Role role) {}

    public record Role(
            Set<String> pubAllowedTopics,
            Set<String> subAllowedTopics
    ) {}
}
