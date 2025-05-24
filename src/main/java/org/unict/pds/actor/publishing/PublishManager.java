package org.unict.pds.actor.publishing;

import akka.actor.*;
import akka.pattern.Patterns;
import akka.routing.ConsistentHashingPool;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import lombok.extern.slf4j.Slf4j;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.PublishManagerConfiguration;
import org.unict.pds.logging.LoggingUtils;
import org.unict.pds.message.publish.PublishMessage;
import org.unict.pds.message.topic.CheckTopicExist;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class PublishManager extends AbstractActor {

    private ActorRef publishWorkerRouter;
    private ActorRef topicManager;
    private final Duration checkTopicTimeout = Duration.ofSeconds(3);

    @Override
    public void preStart() {
        PublishManagerConfiguration config = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).publishManagerConfig();

        int numWorkers = config.numberWorkers();

        publishWorkerRouter = getContext().actorOf(
                new ConsistentHashingPool(numWorkers)
                        .withHashMapper(message -> {
                            if (message instanceof PublishMessage.Request req) {
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
                            LoggingUtils.logApplicationEvent(
                                    LoggingUtils.LogLevel.ERROR,
                                    "Could not resolve topic manager: " + throwable.getMessage(),
                                    "PublishManager"
                            );
                        } else {
                            this.topicManager = actorRef;
                            LoggingUtils.logApplicationEvent(
                                    LoggingUtils.LogLevel.INFO,
                                    "Successfully resolved topic manager",
                                    "PublishManager"
                            );
                        }
                    });
        } else {
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.ERROR,
                    "Topic manager path not configured",
                    "PublishManager"
            );
        }
        LoggingUtils.logApplicationEvent(
                LoggingUtils.LogLevel.INFO,
                "PublishManager started with " + numWorkers + " workers",
                "PublishManager"
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(PublishMessage.Request.class, this::handlePublishMessage)
                .build();
    }

    private void handlePublishMessage(PublishMessage.Request request) {
        MqttPublishMessage message = request.message();
        String topic = message.variableHeader().topicName();
        //  int messageId = message.variableHeader().packetId();
        //  boolean requiresAck = message.fixedHeader().qosLevel().value() > 0;
        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                PublishMessage.Request.class,
                getSender().path().toString(),
                getSelf().path().toString(),
                "InternalHandler.PublishManager"
        );
//        if (topicManager == null) {
//            System.out.println("No topic manager available, publishing message directly");
//            forwardToPublishWorker(message);
//            return;
//        }

        CompletableFuture<Object> future = Patterns.ask(
                topicManager,
                new CheckTopicExist.Request(topic),
                checkTopicTimeout
        ).toCompletableFuture();

        future.thenAccept(response -> {
            CheckTopicExist.Response resp = (CheckTopicExist.Response) response;
            boolean topicExists = resp.exists();

            if (topicExists) {
                LoggingUtils.logApplicationEvent(
                        LoggingUtils.LogLevel.INFO,
                        "Topic exists: " + topic + ", forwarding to publish worker",
                        "PublishManager"
                );

                forwardToPublishWorker(message);
            } else {
                LoggingUtils.logApplicationEvent(
                        LoggingUtils.LogLevel.WARN,
                        "Topic does not exist: " + topic + ", rejecting publish",
                        "PublishManager"
                );
            }
        }).exceptionally(ex -> {
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.ERROR,
                    "Error checking topic existence: " + ex.getMessage(),
                    "PublishManager"
            );
            return null;
        });
    }

    private void forwardToPublishWorker(MqttPublishMessage message) {
        PublishMessage.Request request = new PublishMessage.Request(message);
        publishWorkerRouter.tell(request, self());
    }
}