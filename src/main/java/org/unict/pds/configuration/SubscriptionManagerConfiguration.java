package org.unict.pds.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public record SubscriptionManagerConfiguration(String topicManagerAddress) {
    public static SubscriptionManagerConfiguration load() {
        Config config = ConfigFactory.load().getConfig("subscriber-manager");
        return new SubscriptionManagerConfiguration(config.getString("topic-manager"));
    }
}
