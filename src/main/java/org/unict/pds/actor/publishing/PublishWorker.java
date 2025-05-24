package org.unict.pds.actor.publishing;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.pattern.Patterns;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.PublishWorkerConfiguration;
import org.unict.pds.message.publish.PublishMessage;
import org.unict.pds.message.subscribe.SubscriberLookup;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class PublishWorker extends AbstractActor {
    private final Map<String, BlockingQueue<MqttPublishMessage>> topicQueues = new ConcurrentHashMap<>();
    private final Map<String, TopicWorker> topicWorkers = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    
    private ActorRef subscriptionManager;
    private final Duration lookupTimeout = Duration.ofSeconds(10);

    @Override
    public void preStart() {
        PublishWorkerConfiguration config = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).publishWorkerConfig();
                
        getContext().actorSelection(config.subscriberManagerAddress())
                .resolveOne(Duration.ofSeconds(3))
                .whenComplete((actorRef, throwable) -> {
                    if (throwable != null) {
                        System.err.println("PublishWorker could not resolve subscription manager: " + throwable.getMessage());
                    } else {
                        this.subscriptionManager = actorRef;
//                        System.out.println("PublishWorker successfully resolved subscription manager");
                    }
                });
    }

    @Override
    public void postStop() {
        isRunning.set(false);
        topicWorkers.values().forEach(TopicWorker::shutdown);
        executorService.shutdownNow();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(PublishMessage.Request.class, this::processPublishRequest)
                .match(SubscriberLookup.Response.class, this::handleSubscriberLookupResponse)
                .build();
    }

    private void processPublishRequest(PublishMessage.Request request) {
        MqttPublishMessage message = request.message();
        String topic = message.variableHeader().topicName();
        BlockingQueue<MqttPublishMessage> queue = topicQueues.computeIfAbsent(
                topic, 
                k -> new LinkedBlockingQueue<>()
        );
        queue.offer(message);
        ensureTopicWorkerExists(topic);
    }
    
    private void ensureTopicWorkerExists(String topic) {
        if (!topicWorkers.containsKey(topic)) {
            BlockingQueue<MqttPublishMessage> queue = topicQueues.get(topic);
            TopicWorker worker = new TopicWorker(topic, queue);
            topicWorkers.put(topic, worker);
            executorService.submit(worker);
        }
    }
    
    private void handleSubscriberLookupResponse(SubscriberLookup.Response response) {
        String topic = response.topic();
        List<ActorRef> subscribers = response.subscribers();

        System.out.println("Received subscribers for topic: " +  topic +
                ", number of subscribers: " + subscribers.size());
        
        TopicWorker worker = topicWorkers.get(topic);
        if (worker != null) {
            worker.setSubscribers(subscribers);
            worker.processPendingMessages();
        }
    }

    private class TopicWorker implements Runnable {
        private final String topic;
        private final BlockingQueue<MqttPublishMessage> queue;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final BlockingQueue<MqttPublishMessage> pendingMessages = new LinkedBlockingQueue<>();
        private volatile List<ActorRef> subscribers = null;
        private volatile boolean lookupInProgress = false;
        
        public TopicWorker(String topic, BlockingQueue<MqttPublishMessage> queue) {
            this.topic = topic;
            this.queue = queue;
        }
        
        public void shutdown() {
            running.set(false);
        }
        
        public void setSubscribers(List<ActorRef> subscribers) {
            this.subscribers = subscribers;
            lookupInProgress = false;
        }
        
        public void processPendingMessages() {
            MqttPublishMessage pendingMessage;
            while ((pendingMessage = pendingMessages.poll()) != null) {
                distributeMessageToSubscribers(pendingMessage);
            }
        }
        
        @Override
        public void run() {
            while (running.get() && isRunning.get()) {
                try {
                    MqttPublishMessage message = queue.poll(500, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        processMessage(message);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("Error processing message for topic " + topic + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
        
        private void processMessage(MqttPublishMessage message) {
            System.out.println("Processing message for topic: " + topic);
            if (subscribers == null && !lookupInProgress) {
                lookupSubscribers(message);
            } else if (lookupInProgress) {
                pendingMessages.offer(message);
            } else {
                distributeMessageToSubscribers(message);
            }
        }
        
        private void lookupSubscribers(MqttPublishMessage message) {
            if (subscriptionManager != null) {
                lookupInProgress = true;
                System.out.println("Looking up subscribers for topic: " + topic);
                pendingMessages.offer(message);

                SubscriberLookup.Request request = new SubscriberLookup.Request(topic);
                Patterns.ask(subscriptionManager, request, lookupTimeout)
                        .thenApply(response -> (SubscriberLookup.Response) response)
                        .thenAccept(response -> getSelf().tell(response, ActorRef.noSender()))
                        .exceptionally(ex -> {
                            System.out.printf("Error looking up subscribers for topic %s: %s%n", topic, ex.getMessage());
                            lookupInProgress = false;
                            return null;
                        });
            } else {
                System.out.printf("SubscriptionManager not available, cannot distribute message for topic: %s%n", topic);
            }
        }

        private void distributeMessageToSubscribers(MqttPublishMessage message) {
            if (subscribers == null || subscribers.isEmpty()) {
                System.out.printf("No subscribers for topic: %s%n", topic);
                return;
            }
            
            System.out.printf("Distributing message to %s subscribers for topic: %s%n",
                    subscribers.size(), topic);
            
            if (!subscribers.isEmpty()) {
                subscribers.get(0).tell(new PublishMessage.Release(message), getSelf());
            }
            
            for (int i = 1; i < subscribers.size(); i++) {
                // Create a copy of the message with retained content
                MqttPublishMessage messageCopy = copyMqttPublishMessage(message);
                subscribers.get(i).tell(new PublishMessage.Release(messageCopy), getSelf());
            }
        }
        
        private MqttPublishMessage copyMqttPublishMessage(MqttPublishMessage original) {
            if (original.content() != null) {
                original.content().retain();
            }
            
            return new MqttPublishMessage(
                    original.fixedHeader(),
                    original.variableHeader(),
                    original.content()
            );
        }
    }
}