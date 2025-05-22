package org.unict.pds.message.topic;

public record CheckTopicExist(
) {
    public record Request(
            String topicName
    ) {}

    public record Response(
            String topicName,
            boolean exists
    ) {}
}
