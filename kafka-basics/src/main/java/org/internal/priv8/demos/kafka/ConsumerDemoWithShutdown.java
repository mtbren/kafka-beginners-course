package org.internal.priv8.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer!!!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // Set Server properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // Set Consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); //none/earliest/latest

        // Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get a reference to the main thread
        final Thread mainThread = Thread.currentThread();
        log.info("Thread in main - "+Thread.currentThread().getName());
        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Thread in shutdown hook - "+Thread.currentThread().getName());
                log.info("Detected a shutdown, let's exit by calling a consumer.wakeup()...");

                consumer.wakeup(); // After this, next time when the consumer does a poll, it is going to throw a wakeupException

                // Join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            // Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // Poll for data
            while (true) {
                //log.info("Polling...");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5L));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " | Value: " + record.value());
                    log.info("Partition: " + record.partition() + " | Offset: " + record.offset());
                }
            }
        }catch(WakeupException wakeupException){
            log.info("Consumer is starting to shutdown...");
        }catch(Exception e){
            log.error("Unexpected exception in the consumer - "+e);
        }finally {
            // Close the consumer. This will also commit the offsets
            consumer.close();
            log.info("Consumer has now gracefully shutdown!!!");
        }
    }
}
