package org.unict.pds.actor.subscription;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Terminated;
import akka.pattern.Patterns;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.SubscriptionManagerConfiguration;
import org.unict.pds.message.subscribe.SubscriberLookup;
import org.unict.pds.message.topic.CheckTopicExist;
import org.unict.pds.message.subscribe.SubscribeTopicResponse;

import java.time.Duration;
import java.util.List;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

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
                        System.err.println("Could not resolve topic manager: " + throwable.getMessage());
                    } else {
                        this.topicManager = actorRef;
                        System.out.println("SubscriptionManager Successfully resolved topic manager");
                    }
                });
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Terminated.class, this::handleTerminated)
                .match(MqttSubscribeMessage.class, this::processMessage)
                .match(SubscriberLookup.Request.class, this::handleSubscriberLookup)
                .build();
    }

    private void processMessage(MqttSubscribeMessage message) {
        MqttSubscribePayload payload = message.payload();
        List<String> topicNames = payload.topicSubscriptions().stream()
                .map(MqttTopicSubscription::topicFilter)
                .toList();

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

                if (topicExists) {
                    addSubscriber(topic, originalSender);
                }

                if (pendingChecks.decrementAndGet() == 0) {
                    originalSender.tell(new SubscribeTopicResponse(
                            message.idAndPropertiesVariableHeader().messageId(),
                            results
                    ), getSelf());
                }
            }).exceptionally(ex -> {
                results.set(index, MqttReasonCodes.SubAck.UNSPECIFIED_ERROR);
                if (pendingChecks.decrementAndGet() == 0) {
                    originalSender.tell(new SubscribeTopicResponse(
                            message.idAndPropertiesVariableHeader().messageId(),
                            results
                    ), getSelf());
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
            System.out.println("Added subscriber to topic: " + topic +
                    ", total subscribers: " + subscribers.size());
            getContext().watch(subscriber);
        }
    }

    private void handleSubscriberLookup(SubscriberLookup.Request request) {
        String topic = request.topic();
        System.out.printf("Looking up subscribers for topic: %s", topic);
        List<ActorRef> exactSubscribers = topicSubscribers.getOrDefault(topic, new ArrayList<>());
        SubscriberLookup.Response response = new SubscriberLookup.Response(topic, exactSubscribers);
        sender().tell(response, self());
    }

    private void handleTerminated(Terminated terminatedEvent) {
        ActorRef terminatedSubscriber = terminatedEvent.getActor();
        System.out.println("Subscriber terminated: " + terminatedSubscriber);
        topicSubscribers.values().forEach(subscribers -> subscribers.remove(terminatedSubscriber));
    }

}