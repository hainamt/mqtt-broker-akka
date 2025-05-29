package org.unict.pds.configuration;

import akka.actor.AbstractExtensionId;
import akka.actor.ExtendedActorSystem;
import akka.actor.Extension;
import akka.actor.ExtensionIdProvider;
import lombok.Getter;
import org.unict.pds.exception.BrokerConfigurationException;


public class ConfigurationExtension
        extends AbstractExtensionId<ConfigurationExtension.ConfigExt>
        implements ExtensionIdProvider {

    @Getter
    private static final ConfigurationExtension instance = new ConfigurationExtension();

    @Override
    public ConfigurationExtension lookup() {
        return instance;
    }

    @Override
    public ConfigExt createExtension(ExtendedActorSystem system) {
        return ConfigExt.load();
    }

    public record ConfigExt(
                        SecurityConfiguration securityConfig,
                        TCPConfiguration tcpConfig,
                         MQTTManagerConfiguration mqttManagerConfig,
                         TopicManagerConfiguration topicManagerConfig,
                         SubscriptionManagerConfiguration subscriptionManagerConfig,
                         PublishManagerConfiguration publishManagerConfig,
                         PublishWorkerConfiguration publishWorkerConfig) implements Extension {
        public static ConfigExt load() {
            try {
                return new ConfigExt(
                        SecurityConfiguration.load(),
                        TCPConfiguration.load(),
                        MQTTManagerConfiguration.load(),
                        TopicManagerConfiguration.load(),
                        SubscriptionManagerConfiguration.load(),
                        PublishManagerConfiguration.load(),
                        PublishWorkerConfiguration.load()
                );
            } catch (Exception e) {
                throw new BrokerConfigurationException("Failed to load one or more configurations: " + e.getMessage(), e);
            }
        }
    }
}
