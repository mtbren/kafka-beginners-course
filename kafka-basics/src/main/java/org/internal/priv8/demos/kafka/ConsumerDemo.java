package org.internal.priv8.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Kafka Consumer!!!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // Set Server properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");

        // Set Consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","earliest"); //none/earliest/latest

        // Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // Poll for data
        while(true){
            log.info("Polling...");

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(5L));

            for(ConsumerRecord<String,String> record : records){
                log.info("Key: "+record.key()+" | Value: "+record.value());
                log.info("Partition: "+record.partition()+" | Offset: "+record.offset());
            }
        }
    }
}
