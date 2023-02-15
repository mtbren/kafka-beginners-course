package org.internal.priv8.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());
    public static void main(String[] args) {
        log.info("This works!!!");

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size","400");

        // Dont do this for production since this is not very efficient
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=1;j<=30;j++) {
            for (int i = 1; i <= 10; i++) {
                // Create a Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "Msg " + i);

                // Send the message/data to the Producer
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // Will be executed everytime a record is sent successfully or if there is an exception in the send process
                        if (e == null) {
                            // Record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "TimeStamp: " + metadata.timestamp()
                            );
                        } else {
                            log.error("Error while producing ", e);
                        }

                    }
                });
            }
            try {
                Thread.sleep(400);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // Flush the Producer
        // Tells the producer to flush all data and block until its done --> synchronous
        producer.flush();

        // Flush and Close the Producer
        producer.close();
    }
}
