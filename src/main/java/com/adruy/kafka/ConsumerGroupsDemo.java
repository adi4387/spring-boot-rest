package com.adruy.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerGroupsDemo {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupsDemo.class);

    public void consumer() {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "colleague_event";
        final String colleagueEventTopic = "colleague_event_topic";

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(colleagueEventTopic));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(ofMillis(100));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                LOG.info("Key: " + consumerRecord.key() + ", value: " + consumerRecord.value());
                LOG.info("Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset());
            }
        }
    }
}
