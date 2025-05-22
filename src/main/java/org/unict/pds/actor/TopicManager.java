package org.unict.pds.actor.mqtt;

import akka.actor.AbstractActor;
import akka.actor.Props;
import lombok.RequiredArgsConstructor;
import org.unict.pds.message.topic.CheckTopicExist;

import java.util.HashSet;
import java.util.Set;

@RequiredArgsConstructor
public class TopicManager extends AbstractActor {

    private final Boolean autoCreateTopic;
    private final Set<String> existingTopics = new HashSet<>();

    public static Props props(boolean autoCreateTopic) {
        return Props.create(TopicManager.class, autoCreateTopic);
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
