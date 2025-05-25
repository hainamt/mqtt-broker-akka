package org.unict.pds.actor.publishing;

import akka.actor.*;
import akka.pattern.Patterns;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.unict.pds.actor.ActorResolutionUtils;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.PublishManagerConfiguration;
import org.unict.pds.exception.CriticalActorCouldNotBeResolved;
import org.unict.pds.logging.LoggingUtils;
import org.unict.pds.message.publish.PublishMessage;
import org.unict.pds.message.subscribe.SubscriberLookup;
import org.unict.pds.message.topic.CheckTopicExist;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Getter
public class PublishManager extends AbstractActor {

    private ActorRef topicManager;
    private ActorRef subscriptionManager;
    private int numWorkers;
    private Duration lookupTopicTimeout;
    private Duration lookupSubscriberTimeout ;

    @Override
    public void preStart() {
        PublishManagerConfiguration config = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).publishManagerConfig();

        numWorkers = config.numberWorkers();
        lookupTopicTimeout = config.lookupTopicTimeout();
        lookupSubscriberTimeout = config.lookupSubscriberTimeout();

        try {
//            publishWorkerRouter = getContext().actorOf(
//                    new ConsistentHashingPool(numWorkers)
//                            .withHashMapper(message -> {
//                                if (message instanceof PublishMessage.Request req) {
//                                    return req.message().variableHeader().topicName();
//                                }
//                                return null;
//                            })
//                            .props(Props.create(PublishWorker.class)),
//                    "publish-worker-pool");
            for (int i = 0; i < numWorkers; i++) {
                getContext().actorOf(Props.create(PublishWorker.class), "publish-worker-" + i);
            }

            this.topicManager = ActorResolutionUtils.resolveActor(
                    getContext().getSystem(),
                    config.topicManagerAddress(),
                    config.resolutionTimeout(),
                    "PublishManager",
                    true);

            this.subscriptionManager = ActorResolutionUtils.resolveActor(
                    getContext().getSystem(),
                    config.subscriptionManagerAddress(),
                    config.resolutionTimeout(),
                    "PublishManager",
                    true);

        } catch (CriticalActorCouldNotBeResolved e) {
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.ERROR,
                    "PublishManager startup failed: " + e.getMessage(),
                    "PublishManager"
            );
            throw e;
        }
        LoggingUtils.logApplicationEvent(
                LoggingUtils.LogLevel.INFO,
                "PublishManager started with " + numWorkers + " workers, successfully resolved TopicManager",
                "PublishManager"
        );
//        String topicManagerPath = config.topicManagerAddress();
//        if (topicManagerPath != null && !topicManagerPath.isEmpty()) {
//            getContext().actorSelection(topicManagerPath)
//                    .resolveOne(Duration.ofSeconds(3))
//                    .whenComplete((actorRef, throwable) -> {
//                        if (throwable != null) {
//                            LoggingUtils.logApplicationEvent(
//                                    LoggingUtils.LogLevel.ERROR,
//                                    "Could not resolve topic manager: " + throwable.getMessage(),
//                                    "PublishManager"
//                            );
//                        } else {
//                            this.topicManager = actorRef;
//                            LoggingUtils.logApplicationEvent(
//                                    LoggingUtils.LogLevel.INFO,
//                                    "Successfully resolved topic manager",
//                                    "PublishManager"
//                            );
//                        }
//                    });
//        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(PublishMessage.Request.class, this::handlePublishMessage)
                .match(SubscriberLookup.Request.class, this::handleSubscriberLookup)
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
                lookupTopicTimeout
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

                int workerIndex = Math.abs(topic.hashCode()) % numWorkers;
                ActorRef worker = getContext().child("publish-worker-" + workerIndex).get();
                worker.tell(request, getSelf());
//                forwardToPublishWorker(message);
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

    private void handleSubscriberLookup(SubscriberLookup.Request request) {
        String topic = request.topic();
        ActorRef originalSender = getSender();

        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                SubscriberLookup.Request.class,
                getSender().path().toString(),
                getSelf().path().toString(),
                "PublishManager forwarding subscriber lookup for topic: " + topic
        );

        CompletableFuture<Object> future = Patterns.ask(
                subscriptionManager,
                request,
                lookupSubscriberTimeout
        ).toCompletableFuture();

        future.thenAccept(response -> {
            SubscriberLookup.Response resp = (SubscriberLookup.Response) response;
            originalSender.tell(resp, getSelf());

            LoggingUtils.logInternalMessage(
                    LoggingUtils.LogLevel.INFO,
                    SubscriberLookup.Response.class,
                    getSelf().path().toString(),
                    originalSender.path().toString(),
                    "PublishManager forwarded subscriber lookup response for topic: " + topic
            );
        }).exceptionally(ex -> {
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.ERROR,
                    "Error looking up subscribers: " + ex.getMessage(),
                    "PublishManager"
            );
            return null;
        });
    }


//    private void forwardToPublishWorker(MqttPublishMessage message) {
//        PublishMessage.Request request = new PublishMessage.Request(message);
//        publishWorkerRouter.tell(request, self());
//    }
}