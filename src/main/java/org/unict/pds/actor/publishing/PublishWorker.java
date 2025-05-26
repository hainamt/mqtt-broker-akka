package org.unict.pds.actor.publishing;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.unict.pds.logging.LoggingUtils;
import org.unict.pds.message.publish.PublishMessage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class PublishWorker extends AbstractActor {
    private final Map<String, BlockingQueue<PublishTask>> topicQueues = new ConcurrentHashMap<>();
    private final Map<String, TopicWorker> topicWorkers = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    @Override
    public void postStop() {
        isRunning.set(false);
        topicWorkers.values().forEach(TopicWorker::shutdown);
        executorService.shutdownNow();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(PublishMessage.RequestWithSubscribers.class, this::processPublishRequestWithSubscribers)
                .build();
    }


    private void processPublishRequestWithSubscribers(PublishMessage.RequestWithSubscribers request) {
        MqttPublishMessage message = request.message();
        String topic = message.variableHeader().topicName();
        List<ActorRef> subscribers = request.subscribers();
        
        PublishTask task = new PublishTask(message, subscribers);
        BlockingQueue<PublishTask> queue = topicQueues.computeIfAbsent(
                topic, 
                k -> new LinkedBlockingQueue<>()
        );
        queue.offer(task);
        ensureTopicWorkerExists(topic);
    }
    
    private void ensureTopicWorkerExists(String topic) {
        if (!topicWorkers.containsKey(topic)) {
            BlockingQueue<PublishTask> queue = topicQueues.get(topic);
            TopicWorker worker = new TopicWorker(topic, queue);
            topicWorkers.put(topic, worker);
            executorService.submit(worker);
        }
    }

    private record PublishTask(MqttPublishMessage message, List<ActorRef> subscribers) {}

    private class TopicWorker implements Runnable {
        private final String topic;
        private final BlockingQueue<PublishTask> queue;
        private final AtomicBoolean running = new AtomicBoolean(true);
        
        public TopicWorker(String topic, BlockingQueue<PublishTask> queue) {
            this.topic = topic;
            this.queue = queue;
        }
        
        public void shutdown() {
            running.set(false);
        }
        
        @Override
        public void run() {
            while (running.get() && isRunning.get()) {
                try {
                    PublishTask task = queue.poll(500, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        distributeMessageToSubscribers(task.message, task.subscribers);
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
        
        private void distributeMessageToSubscribers(MqttPublishMessage message, List<ActorRef> subscribers) {
            if (subscribers == null || subscribers.isEmpty()) {
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