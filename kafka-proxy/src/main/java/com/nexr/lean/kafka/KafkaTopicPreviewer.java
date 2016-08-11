package com.nexr.lean.kafka;

import com.nexr.lean.kafka.util.SimpleKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class KafkaTopicPreviewer {

    public static final String GROUP_ID = "previewer";
    private static Logger log = LoggerFactory.getLogger(KafkaTopicPreviewer.class);
    private final String brokers;
    private SimpleKafkaConsumer simpleKafkaConsumer;

    public KafkaTopicPreviewer(String brokers) {
        try {
            this.brokers = brokers;
            this.simpleKafkaConsumer = new SimpleKafkaConsumer(brokers);
        } catch (Exception e) {
            throw new ConfigException("Fail to initialize SchemaRegitry", e);
        }
    }

    /**
     * Fetch the records of {@code topic} from the earliest offset.
     *
     * @param topic           a topic to retrieve
     * @param timeout         a time for expiration
     * @param rowNumber       a number of record
     * @param consumerConfigs a properties for the {@linkplain org.apache.kafka.clients.consumer.KafkaConsumer}.
     *                        <p> This properties should include the bootstrap-server, key-deserializer, value-deserializer and so on.
     * @param valueClass      a value class of the {@code ConsumerRecord}
     * @param <V>
     * @return the records of {@code topic}
     */
    public <V> List<ConsumerRecord<String, V>> fetch(String topic, long timeout, int rowNumber, Properties consumerConfigs,
                                                     Class<V> valueClass) {
        Properties properties = new Properties(consumerConfigs);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, V>> datas = simpleKafkaConsumer.fetchSync(topic, timeout, rowNumber, consumerConfigs, valueClass);
        return datas;
    }

}
