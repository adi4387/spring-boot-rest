package com.adruy.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerDemoWithCallback {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public void produce() {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("colleague_event_topic", "Hello Kafka");
        for (int i = 0; i < 10; i++) {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Error while publishing", exception);
                } else {
                    LOG.info("Successfully published message to. \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "timestamp: " + metadata.timestamp());
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
