package org.unict.pds.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;

public record PublishManagerConfiguration(
        String topicManagerAddress,
        String subscriptionManagerAddress,
        int numberWorkers,
        Duration lookupTopicTimeout,
        Duration lookupSubscriberTimeout,
        Duration resolutionTimeout
) implements LoadableActorConfig {
    public static PublishManagerConfiguration load() {
        Config config = ConfigFactory.load().getConfig("publish-manager");
        Config commonConfig = ConfigFactory.load().getConfig("common");
        return new PublishManagerConfiguration(
                config.getString("topic-manager"),
                config.getString("subscription-manager"),
                config.getInt("number-workers"),
                Duration.ofSeconds(config.hasPath("lookup-topic-timeout")?
                        config.getInt("lookup-topic-timeout"): 5),
                Duration.ofSeconds(config.hasPath("lookup-subscriber-timeout")?
                        config.getInt("lookup-subscriber-timeout"): 5),
                Duration.ofSeconds(config.hasPath("resolution-timeout")?
                        config.getInt("resolution-timeout") : commonConfig.getInt("resolution-timeout"))
        );
    }

}
