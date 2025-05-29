package org.unict.pds.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;

public record MQTTManagerConfiguration(
        String subscriptionManagerAddress,
        String publishManagerAddress,
        String authenticatorAddress,
        Duration resolutionTimeout
) implements LoadableActorConfig {
    public static MQTTManagerConfiguration load() {
        Config config = ConfigFactory.load().getConfig("mqtt-manager");
        Config commonConfig = ConfigFactory.load().getConfig("common");
        String authenticatorAddress = config.hasPath("authenticator")? config.getString("authenticator") : null;
        return new MQTTManagerConfiguration(
                config.getString("subscription-manager"),
                config.getString("publish-manager"),
                authenticatorAddress,
                Duration.ofSeconds(config.hasPath("resolution-timeout")?
                        config.getInt("resolution-timeout") : commonConfig.getInt("resolution-timeout"))
        );
    }
}
