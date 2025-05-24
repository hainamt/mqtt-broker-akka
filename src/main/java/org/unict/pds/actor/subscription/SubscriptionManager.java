package org.unict.pds.actor.subscription;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Terminated;
import akka.pattern.Patterns;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.SubscriptionManagerConfiguration;
import org.unict.pds.logging.LoggingUtils;
import org.unict.pds.message.subscribe.SubscribeMessage;
import org.unict.pds.message.subscribe.SubscriberLookup;
import org.unict.pds.message.subscribe.UnsubscribeMessage;
import org.unict.pds.message.topic.CheckTopicExist;

import java.time.Duration;
import java.util.List;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class SubscriptionManager extends AbstractActor {

    private ActorRef topicManager;
    private final Map<String, List<ActorRef>> topicSubscribers = new ConcurrentHashMap<>();

    @Override
    public void preStart() {

        SubscriptionManagerConfiguration subscriptionManagerConfig = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).subscriptionManagerConfig();

        ActorSelection selection = getContext().actorSelection(subscriptionManagerConfig.topicManagerAddress());
        selection.resolveOne(java.time.Duration.ofSeconds(3))
                .whenComplete((actorRef, throwable) -> {
                    if (throwable != null) {
                        LoggingUtils.logApplicationEvent(
                                LoggingUtils.LogLevel.ERROR,
                                "Could not resolve topic manager: " + throwable.getMessage(),
                                "SubscriptionManager"
                        );
                    } else {
                        this.topicManager = actorRef;
                        LoggingUtils.logApplicationEvent(
                                LoggingUtils.LogLevel.INFO,
                                "Successfully resolved topic manager",
                                "SubscriptionManager"
                        );
                    }
                });
        LoggingUtils.logApplicationEvent(
                LoggingUtils.LogLevel.INFO,
                "SubscriptionManager started",
                "SubscriptionManager"
        );

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Terminated.class, this::handleTerminated)
                .match(SubscribeMessage.Request.class, this::handleSubscribeMessage)
                .match(SubscriberLookup.Request.class, this::handleSubscriberLookup)
                .match(UnsubscribeMessage.Request.class, this::handleUnsubscribeMessage)
                .build();
    }

    private void handleUnsubscribeMessage(UnsubscribeMessage.Request request) {
        MqttUnsubscribeMessage message = request.message();
        List<String> topicNames = message.payload().topics();

        final ActorRef originalSender = sender();
        AtomicInteger pendingChecks = new AtomicInteger(topicNames.size());
        List<MqttReasonCodes.UnsubAck> results = new ArrayList<>(Collections.nCopies(topicNames.size(), null));

        for (int i = 0; i < topicNames.size(); i++) {
            final String topic = topicNames.get(i);

            boolean removed = removeSubscriber(topic, originalSender);

            results.set(i, removed ?
                    MqttReasonCodes.UnsubAck.SUCCESS :
                    MqttReasonCodes.UnsubAck.NO_SUBSCRIPTION_EXISTED);

            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.INFO,
                    "Unsubscribe from topic '" + topic + "': " +
                            (removed ? "SUCCESS" : "NO_SUBSCRIPTION_EXISTED"),
                    "SubscriptionManager"
            );

            if (pendingChecks.decrementAndGet() == 0) {
                originalSender.tell(new UnsubscribeMessage.Response(
                        message.variableHeader().messageId(),
                        results),
                        getSelf());
            }

            LoggingUtils.logInternalMessage(
                    LoggingUtils.LogLevel.INFO,
                    UnsubscribeMessage.Response.class,
                    self().path().toString(),
                    originalSender.path().toString(),
                    "SubscriptionManager.InternalHandler"
            );

        }
    }

    private void handleSubscribeMessage(SubscribeMessage.Request request) {
        MqttSubscribeMessage message = request.message();
        List<String> topicNames = message.payload().topicSubscriptions().stream()
                .map(MqttTopicSubscription::topicFilter)
                .toList();

        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                SubscribeMessage.Request.class,
                sender().path().toString(),
                self().path().toString(),
                "InternalHandler.SubscriptionManager"
        );
        final ActorRef originalSender = sender();
        AtomicInteger pendingChecks = new AtomicInteger(topicNames.size());
        List<MqttReasonCodes.SubAck> results = new ArrayList<>(Collections.nCopies(topicNames.size(), null));

        for (int i = 0; i < topicNames.size(); i++) {
            final int index = i;
            final String topic = topicNames.get(i);

            CompletableFuture<Object> future = Patterns.ask(
                    topicManager,
                    new CheckTopicExist.Request(topic),
                    Duration.ofSeconds(3)
            ).toCompletableFuture();

            future.thenAccept(response -> {
                CheckTopicExist.Response resp = (CheckTopicExist.Response) response;
                boolean topicExists = resp.exists();

                results.set(index, resp.exists() ?
                        MqttReasonCodes.SubAck.GRANTED_QOS_0 :
                        MqttReasonCodes.SubAck.UNSPECIFIED_ERROR);

                LoggingUtils.logApplicationEvent(
                        LoggingUtils.LogLevel.INFO,
                        "Topic check result for '" + topic + "': " +
                                (topicExists ? "exists" : "does not exist"),
                        "SubscriptionManager"
                );

                if (topicExists) {
                    addSubscriber(topic, originalSender);
                }

                if (pendingChecks.decrementAndGet() == 0) {
                    originalSender.tell(new SubscribeMessage.Response(
                            message.idAndPropertiesVariableHeader().messageId(),
                            results
                    ), getSelf());
                }
            }).exceptionally(ex -> {
                results.set(index, MqttReasonCodes.SubAck.UNSPECIFIED_ERROR);
                LoggingUtils.logApplicationEvent(
                        LoggingUtils.LogLevel.ERROR,
                        "Topic check error for '" + topic + "': " + ex.getMessage(),
                        "SubscriptionManager"
                );


                if (pendingChecks.decrementAndGet() == 0) {
                    originalSender.tell(new SubscribeMessage.Response(
                            message.idAndPropertiesVariableHeader().messageId(),
                            results
                    ), getSelf());
                    LoggingUtils.logInternalMessage(
                            LoggingUtils.LogLevel.INFO,
                            SubscribeMessage.Response.class,
                            self().path().toString(),
                            originalSender.path().toString(),
                            "Sent subscribe response for " + topicNames.size() + " topics"
                    );
                }
                return null;
            });
        }
    }


    private void addSubscriber(String topic, ActorRef subscriber) {
        List<ActorRef> subscribers = topicSubscribers.computeIfAbsent(
                topic, k -> new CopyOnWriteArrayList<>());

        if (!subscribers.contains(subscriber)) {
            subscribers.add(subscriber);
//            System.out.println("Added subscriber to topic: " + topic +
//                    ", total subscribers: " + subscribers.size());
            getContext().watch(subscriber);
        }
    }

    private boolean removeSubscriber(String topic, ActorRef subscriber) {
        List<ActorRef> subscribers = topicSubscribers.get(topic);
        if (subscribers != null) {
            boolean removed = subscribers.remove(subscriber);
            if (topicSubscribers.values().stream().noneMatch(list -> list.contains(subscriber))) {
                getContext().unwatch(subscriber);
            }
            if (removed) {
                System.out.println("Removed subscriber from topic: " + topic +
                        ", total subscribers: " + subscribers.size());
            }
            return removed;
        } else {
            return false;
        }
    }

    private void handleSubscriberLookup(SubscriberLookup.Request request) {
        String topic = request.topic();
        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                SubscriberLookup.Request.class,
                sender().path().toString(),
                self().path().toString(),
                "Looking up subscribers for topic: " + topic
        );

        List<ActorRef> exactSubscribers = topicSubscribers.getOrDefault(topic, new ArrayList<>());
        SubscriberLookup.Response response = new SubscriberLookup.Response(topic, exactSubscribers);
        LoggingUtils.logApplicationEvent(
                LoggingUtils.LogLevel.INFO,
                "Found " + exactSubscribers.size() + " subscribers for topic: " + topic,
                "SubscriptionManager"
        );
        LoggingUtils.logInternalMessage(
                LoggingUtils.LogLevel.INFO,
                SubscriberLookup.Response.class,
                self().path().toString(),
                sender().path().toString(),
                "Sent subscriber lookup response for topic: " + topic
        );
        sender().tell(response, self());
    }

    private void handleTerminated(Terminated terminatedEvent) {
        ActorRef terminatedSubscriber = terminatedEvent.getActor();
        LoggingUtils.logApplicationEvent(
                LoggingUtils.LogLevel.INFO,
                "Subscriber terminated: " + terminatedSubscriber,
                "SubscriptionManager"
        );
        topicSubscribers.values().forEach(subscribers -> subscribers.remove(terminatedSubscriber));
    }

}