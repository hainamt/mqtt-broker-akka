package org.unict.pds.actor;

import akka.actor.AbstractActor;
import lombok.Getter;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.TopicManagerConfiguration;
import org.unict.pds.logging.LoggingUtils;
import org.unict.pds.message.topic.CheckTopicExist;

import java.util.HashSet;
import java.util.Set;

@Getter
public class TopicManager extends AbstractActor {

    private boolean autoCreateTopic;
    private final Set<String> existingTopics = new HashSet<>();

    @Override
    public void preStart() {

        TopicManagerConfiguration topicManagerConfiguration = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).topicManagerConfig();
        autoCreateTopic = topicManagerConfiguration.autoCreateTopic();

        LoggingUtils.logApplicationEvent(
                LoggingUtils.LogLevel.INFO,
                "TopicManager started with autoCreateTopic=" + autoCreateTopic,
                "TopicManager"
        );

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CheckTopicExist.Request.class,
                        this::handleCheckTopicExists
                        )
                .build();
    }

    private void handleCheckTopicExists(CheckTopicExist.Request request) {
        boolean topicExists = existingTopics.contains(request.topicName());

        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                CheckTopicExist.Request.class,
                getSender().path().toString(),
                getSelf().path().toString(),
                "Checking if topic exists: " + request.topicName()
        );

        if (!topicExists && autoCreateTopic) {
            existingTopics.add(request.topicName());
            topicExists = true;

            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.INFO,
                    "Auto-created new topic: " + request.topicName() + " (autoCreateTopic=true)",
                    "TopicManager"
            );

        }
        getSender().tell(new CheckTopicExist.Response(request.topicName(), topicExists), getSelf());

        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                CheckTopicExist.Response.class,
                getSelf().path().toString(),
                getSender().path().toString(),
                "Sent topic check response for: " + request.topicName() + ", exists: " + topicExists
        );

    }
}
