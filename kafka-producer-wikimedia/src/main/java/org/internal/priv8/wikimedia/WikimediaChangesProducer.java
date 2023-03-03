package org.internal.priv8.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) {
        String bootstrapServer = "localhost:9092";
        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        // Set Server properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",bootstrapServer);

        // Set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer,topic);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler,eventSourceBuilder);
        BackgroundEventSource eventSource = builder.build();

        // Start the Producer in another thread
        eventSource.start();

        // We produce for 10 min and block the program until then
        try {
            TimeUnit.MINUTES.sleep(3);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
