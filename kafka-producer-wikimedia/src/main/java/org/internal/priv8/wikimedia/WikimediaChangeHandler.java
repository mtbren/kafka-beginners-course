package org.internal.priv8.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class WikimediaChangeHandler implements BackgroundEventHandler {

    private KafkaProducer<String, String> kafkaProducer;
    private String topic;

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen(){
        // Don't need to do anything here
    }

    @Override
    public void onClosed(){
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent){
        log.info(messageEvent.getData());
        // asynchronous
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment){
        // Don't need to do anything here
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream reading => ",t);
    }
}
