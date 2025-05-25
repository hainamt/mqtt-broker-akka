package org.unict.pds.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;


public record SubscriptionManagerConfiguration(String topicManagerAddress,
                                               Duration resolutionTimeout) implements LoadableActorConfig {

    public static SubscriptionManagerConfiguration load() {
        Config config = ConfigFactory.load().getConfig("subscription-manager");
        Config commonConfig = ConfigFactory.load().getConfig("common");
        return new SubscriptionManagerConfiguration(
                config.getString("topic-manager"),
                Duration.ofSeconds(config.hasPath("resolution-timeout") ?
                        config.getInt("resolution-timeout") : commonConfig.getInt("resolution-timeout")));
    }
}
