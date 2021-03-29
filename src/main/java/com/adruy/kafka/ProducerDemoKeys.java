package com.adruy.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerDemoKeys {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public void produce() throws ExecutionException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            final String topic = "colleague_event_topic";
            final String key = "id_" + i;
            final String value = "Hello Kafka";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            LOG.info("Key: " + key);

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
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
