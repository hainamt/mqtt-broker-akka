package org.unict.pds.actor;

import akka.actor.AbstractActor;
import lombok.Getter;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.TopicManagerConfiguration;
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
        if (!topicExists && autoCreateTopic) {
            existingTopics.add(request.topicName());
            topicExists = true;
        }
        getSender().tell(new CheckTopicExist.Response(request.topicName(), topicExists), getSelf());
    }
}
