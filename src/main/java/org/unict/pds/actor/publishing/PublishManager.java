package org.unict.pds.actor.publishing;

import akka.actor.*;
import akka.pattern.Patterns;
import akka.routing.ConsistentHashingPool;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.PublishManagerConfiguration;
import org.unict.pds.message.publish.PublishManagerResponse;
import org.unict.pds.message.publish.PublishMessageRequest;
import org.unict.pds.message.topic.CheckTopicExist;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class PublishManager extends AbstractActor {

    private ActorRef publishWorkerRouter;
    private ActorRef topicManager;
    private final Duration checkTopicTimeout = Duration.ofSeconds(3);


    public static Props props() {
        return Props.create(PublishManager.class);
    }

    public static Props props(int numWorkers) {
        return Props.create(PublishManager.class, numWorkers);
    }

    @Override
    public void preStart() {
        PublishManagerConfiguration config = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).publishManagerConfig();

        int numWorkers = config.numberWorkers();

        publishWorkerRouter = getContext().actorOf(
                new ConsistentHashingPool(numWorkers)
                        .withHashMapper(message -> {
                            if (message instanceof PublishMessageRequest req) {
                                return req.message().variableHeader().topicName();
                            }
                            return null;
                        })
                        .props(Props.create(PublishWorker.class)),
                "publish-worker-pool");

        String topicManagerPath = config.topicManagerAddress();
        if (topicManagerPath != null && !topicManagerPath.isEmpty()) {
            getContext().actorSelection(topicManagerPath)
                    .resolveOne(Duration.ofSeconds(3))
                    .whenComplete((actorRef, throwable) -> {
                        if (throwable != null) {
                            System.err.println("Could not resolve topic manager: " + throwable.getMessage());
                        } else {
                            this.topicManager = actorRef;
                            System.out.println("Successfully resolved topic manager");
                        }
                    });
        } else {
            System.err.println("Topic manager path not configured");
        }

        System.out.println("PublishManager started with " + numWorkers + " workers");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MqttPublishMessage.class, this::handlePublishMessage)
                .build();
    }

    private void handlePublishMessage(MqttPublishMessage message) {
        String topic = message.variableHeader().topicName();
        int messageId = message.variableHeader().packetId();
//        boolean requiresAck = message.fixedHeader().qosLevel().value() > 0;
        boolean requiresAck = false;
        System.out.println("PublishManager received message for topic: " + topic);

        if (topicManager == null) {
            System.out.println("No topic manager available, publishing message directly");
            forwardToPublishWorker(message, requiresAck, messageId);
            return;
        }

        CompletableFuture<Object> future = Patterns.ask(
                topicManager,
                new CheckTopicExist.Request(topic),
                checkTopicTimeout
        ).toCompletableFuture();

        future.thenAccept(response -> {
            CheckTopicExist.Response resp = (CheckTopicExist.Response) response;
            boolean topicExists = resp.exists();

            if (topicExists) {
                System.out.println("Topic exists: " + topic + ", forwarding to publish worker");
                forwardToPublishWorker(message, requiresAck, messageId);
            } else {
                System.out.println("Topic does not exist: " + topic + ", rejecting publish");
                if (requiresAck) {
                    sender().tell(
                            new PublishManagerResponse(messageId, false),
                            self()
                    );
                }
            }
        }).exceptionally(ex -> {
            System.err.println("Error checking topic existence: " + ex.getMessage());
            if (requiresAck) {
                sender().tell(
                        new PublishManagerResponse(messageId, false),
                        self()
                );
            }
            return null;
        });
    }

    private void forwardToPublishWorker(MqttPublishMessage message, boolean requiresAck, int messageId) {
        PublishMessageRequest request = new PublishMessageRequest(message);
        publishWorkerRouter.tell(request, self());

        if (requiresAck) {
            sender().tell(
                    new PublishManagerResponse(messageId, true),
                    self()
            );
        }
    }
}