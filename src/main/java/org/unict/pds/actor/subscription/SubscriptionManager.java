package org.unict.pds.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.pattern.Patterns;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.SubscriptionManagerConfiguration;
import org.unict.pds.message.topic.CheckTopicExist;
import org.unict.pds.message.topic.SubscriptionManagerResponse;

import java.time.Duration;
import java.util.List;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscriptionManager extends AbstractActor {

    private ActorRef topicManager;

    @Override
    public void preStart() {

        SubscriptionManagerConfiguration subscriptionManagerConfig = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).getSubscriptionManagerConfig();

        ActorSelection selection = getContext().actorSelection(subscriptionManagerConfig.topicManagerAddress());

        selection.resolveOne(java.time.Duration.ofSeconds(3))
                .whenComplete((actorRef, throwable) -> {
                    if (throwable != null) {
                        System.err.println("Could not resolve topic manager: " + throwable.getMessage());
                    } else {
                        this.topicManager = actorRef;
                        System.out.println("Successfully resolved topic manager");
                    }
                });
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(
                        MqttSubscribeMessage.class,
                        this::processMessage
                )
                .build();
    }

    private void processMessage(MqttSubscribeMessage message) {
        MqttSubscribePayload payload = message.payload();
        List<String> topicNames = payload.topicSubscriptions().stream()
                .map(MqttTopicSubscription::topicFilter)
                .toList();

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
                results.set(index, resp.exists() ?
                        MqttReasonCodes.SubAck.GRANTED_QOS_0 :
                        MqttReasonCodes.SubAck.UNSPECIFIED_ERROR);
                if (pendingChecks.decrementAndGet() == 0) {
                    getSender().tell(new SubscriptionManagerResponse(
                            message.idAndPropertiesVariableHeader().messageId(),
                            results
                    ), getSelf());
                }
            }).exceptionally(ex -> {
                results.set(index, MqttReasonCodes.SubAck.UNSPECIFIED_ERROR);
                if (pendingChecks.decrementAndGet() == 0) {
                    getSender().tell(new SubscriptionManagerResponse(
                            message.idAndPropertiesVariableHeader().messageId(),
                            results
                    ), getSelf());
                }
                return null;
            });
        }
    }
}