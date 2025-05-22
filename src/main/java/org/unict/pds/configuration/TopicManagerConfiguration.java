package org.unict.pds.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public record TopicManagerConfiguration(boolean autoCreateTopic) {
    public static TopicManagerConfiguration load() {
        Config config = ConfigFactory.load().getConfig("topic-manager");
        return new TopicManagerConfiguration(
                config.getBoolean("auto-create-topic")
        );
    }
}
