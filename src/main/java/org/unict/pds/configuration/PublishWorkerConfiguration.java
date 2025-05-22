package org.unict.pds.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;

public record PublishWorkerConfiguration(
        String subscriberManagerAddress,
        Duration subscriberLookupTimeoutSecond
) {
    public static PublishWorkerConfiguration load() {
        Config config = ConfigFactory.load().getConfig("publish-worker");
        return new PublishWorkerConfiguration(
                config.getString("subscription-manager"),
                config.getDuration("subscriber-lookup-timeout-second")
        );
    }
}
