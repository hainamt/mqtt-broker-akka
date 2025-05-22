package org.unict.pds.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public record MQTTManagerConfiguration(
        String subscriptionManagerAddress,
        String publishManagerAddress
) {
    public static MQTTManagerConfiguration load() {
        Config config = ConfigFactory.load().getConfig("mqtt-manager");
        return new MQTTManagerConfiguration(
                config.getString("subscription-manager"),
                config.getString("publish-manager")
        );
    }
}
