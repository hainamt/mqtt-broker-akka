package org.unict.pds.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public record PublishManagerConfiguration(
        String topicManagerAddress,
        int numberWorkers
) {
    public static PublishManagerConfiguration load() {
        Config config = ConfigFactory.load().getConfig("publish-manager");
        return new PublishManagerConfiguration(
                config.getString("topic-manager"),
                config.getInt("number-workers")
        );
    }

}
