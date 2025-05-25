package org.unict.pds.actor.publishing;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.unict.pds.logging.LoggingUtils;
import org.unict.pds.message.publish.PublishMessage;
import org.unict.pds.message.subscribe.SubscriberLookup;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class PublishWorker extends AbstractActor {
    private final Map<String, BlockingQueue<MqttPublishMessage>> topicQueues = new ConcurrentHashMap<>();
    private final Map<String, TopicWorker> topicWorkers = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    private ActorRef publishManager;

    @Override
    public void preStart() {
//        PublishWorkerConfiguration config = ConfigurationExtension.getInstance()
//                .get(getContext().getSystem()).publishWorkerConfig();
        publishManager = getContext().parent();
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

        LoggingUtils.logApplicationEvent(
                LoggingUtils.LogLevel.INFO,
                "Received subscribers for topic: " + topic + ", number of subscribers: " + subscribers.size(),
                "PublishWorker"
        );

        
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
                    LoggingUtils.logApplicationEvent(
                            LoggingUtils.LogLevel.ERROR,
                            "Error processing message for topic " + topic + ": " + e.getMessage(),
                            "PublishWorker"
                    );
                    throw e;
                }
            }
        }
        
        private void processMessage(MqttPublishMessage message) {
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.INFO,
                    "Processing message for topic: " + topic,
                    "PublishWorker"
            );

            if (subscribers == null && !lookupInProgress) {
                lookupSubscribers(message);
            } else if (lookupInProgress) {
                pendingMessages.offer(message);
            } else {
                distributeMessageToSubscribers(message);
            }
        }

        private void lookupSubscribers(MqttPublishMessage message) {
            lookupInProgress = true;
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.INFO,
                    "Looking up subscribers for topic: " + topic,
                    "PublishWorker"
            );
            pendingMessages.offer(message);
            SubscriberLookup.Request request = new SubscriberLookup.Request(topic);
            publishManager.tell(request, getSelf());
        }

        private void distributeMessageToSubscribers(MqttPublishMessage message) {
            if (subscribers == null || subscribers.isEmpty()) {
//                LoggingUtils.logApplicationEvent(
//                        LoggingUtils.LogLevel.INFO,
//                        "No subscribers for topic:" + topic,
//                        "PublishWorker"
//                );
                return;
            }

            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.INFO,
                    "Distributing message to " + subscribers.size() + " subscribers for topic: " + topic,
                    "PublishWorker"
            );
            
            if (!subscribers.isEmpty()) {
                subscribers.get(0).tell(new PublishMessage.Release(message), getSelf());
            }
            
            for (int i = 1; i < subscribers.size(); i++) {
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