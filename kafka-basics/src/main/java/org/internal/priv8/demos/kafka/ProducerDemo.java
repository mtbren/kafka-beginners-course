package org.internal.priv8.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Kafka Producer!!!");

        // Set Server properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");

        // Set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java","Day 3 with Kafka producer");

        // Send the message/data to the Producer
        producer.send(producerRecord);

        // Flush the Producer
        // Tells the producer to flush all data and block until its done --> synchronous
        producer.flush();

        // Flush and Close the Producer
        producer.close();
    }
}
