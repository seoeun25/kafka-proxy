package com.nexr.lean.kafka;

import com.nexr.lean.kafka.consumer.SimpleConsumerConfig;
import com.nexr.lean.kafka.consumer.ConsumerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class TopicPreviewer {

    public static final String GROUP_ID = "previewer";
    private static Logger log = LoggerFactory.getLogger(TopicPreviewer.class);
    private final String brokers;
    private ConsumerService consumerService;

    public TopicPreviewer(String brokers) {
        try {
            this.brokers = brokers;
            this.consumerService = ConsumerService.getInstance();
        } catch (Exception e) {
            throw new ConfigException("Fail to initialize SchemaRegistry", e);
        }
    }

    /**
     * Fetch the records of {@code topic} from the earliest offset.
     *
     * @param topic           a topic to retrieve
     * @param timeout         a time for expiration
     * @param minRowNumber    a number of record
     * @param consumerConfigs a properties for the {@linkplain org.apache.kafka.clients.consumer.KafkaConsumer}.
     *                        <p> This properties should include the bootstrap-server, key-deserializer, value-deserializer and so on.
     * @param valueClass      a value class of the {@code ConsumerRecord}
     * @param <V>
     * @return the records of {@code topic}. If there are lots of messages in topic, the records could be
     * greater than rowNumber. Otherwise, If there are messages fewer than rowNumber, the records are the
     * all message but size is smaller than rowNumber.
     */
    public <V> List<ConsumerRecord<String, V>> fetch(String topic, long timeout, int minRowNumber, Properties consumerConfigs,
                                                     Class<V> valueClass) {
        Properties properties = new Properties(consumerConfigs);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigs.put(SimpleConsumerConfig.ENABLE_MANUAL_COMMIT_CONFIG, "false");
        List<ConsumerRecord<String, V>> datas = consumerService.fetchSync(topic, timeout, minRowNumber, consumerConfigs, valueClass);
        return datas;
    }

}
