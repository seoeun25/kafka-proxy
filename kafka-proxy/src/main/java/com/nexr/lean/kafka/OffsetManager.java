package com.nexr.lean.kafka;

import com.nexr.lean.kafka.common.KafkaProxyRuntimeException;
import com.nexr.lean.kafka.common.OffsetInfo;
import com.nexr.lean.kafka.common.Utils;
import com.nexr.lean.kafka.util.SimpleConsumerConfig;
import com.nexr.lean.kafka.util.SimpleKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class OffsetManager {

    private static Logger log = LoggerFactory.getLogger(OffsetManager.class);
    private String zkServers;
    private String brokers;
    private SimpleKafkaConsumer simpleKafkaConsumer;

    public OffsetManager(String zkServers, String brokers) {
        this.zkServers = zkServers;
        this.brokers = brokers;
        this.simpleKafkaConsumer = new SimpleKafkaConsumer(brokers);
    }

    /**
     * Gets the last committed offsets for a given topic and group
     *
     * @param topic   the topic to check
     * @param groupId the group of consumer
     * @return the last committed offsets
     * @throws KafkaProxyRuntimeException if topic does not exist.
     */
    public Map<TopicPartition, OffsetAndMetadata> getCommittedOffset(String topic, String groupId) {
        if (!KafkaTopicManager.topicExists(zkServers, topic)) {
            throw new KafkaProxyRuntimeException(String.format("Topic[%s] not found", topic));
        }
        return simpleKafkaConsumer.getCommittedOffset(topic, groupId, Utils.keyValueToProperties(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                SimpleConsumerConfig.ENABLE_MANUAL_COMMIT_CONFIG, "false"
        ));
    }

    /**
     * Gets the last offset for a given topic.
     *
     * @param topic the topic to check
     * @return the last offsets
     * @throws KafkaProxyRuntimeException if topic does not exist.
     */
    public Map<TopicPartition, Long> getEndOffset(String topic) {
        if (!KafkaTopicManager.topicExists(zkServers, topic)) {
            throw new KafkaProxyRuntimeException(String.format("Topic[%s] not found", topic));
        }
        String groupId = "eo-" + Utils.randomString(8);
        return simpleKafkaConsumer.getEndOffset(topic, groupId, Utils.keyValueToProperties(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, "eo-" + Utils.randomString(8),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                SimpleConsumerConfig.ENABLE_MANUAL_COMMIT_CONFIG, "false"
        ));
    }

    /**
     * Gets the offset info for a given topic.
     *
     * @param topic   the topic to check
     * @param groupId the group of consumer
     * @return the map of offset info. OffsetInfo contains committed offset and end offset.
     * @throws KafkaProxyRuntimeException if topic does not exist. Or, if the partition info is not consistent.
     */
    public Map<TopicPartition, OffsetInfo> getOffsets(String topic, String groupId) {
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = getCommittedOffset(topic, groupId);
        Map<TopicPartition, Long> endOffsets = getEndOffset(topic);
        if (committedOffsets.size() != endOffsets.size()) {
            String errMessage = String.format("PartitionInfo is not consistent for %s, group=%s:  " +
                    "CommittedOffset tp=%s, EndOffset tp=%s", topic, groupId, committedOffsets.size(), endOffsets.size());
            throw new KafkaProxyRuntimeException(errMessage);
        }
        Map<TopicPartition, OffsetInfo> offsetInfoMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            long committedOffset = -1;
            if (committedOffsets.containsKey(entry.getKey())) {
                committedOffset = committedOffsets.get(entry.getKey()) == null ? -1 :
                        committedOffsets.get(entry.getKey()).offset();
            } else {
                log.warn("Fail to get committed offset for tp: topic={}, partition={}, endOffset={}",
                        entry.getKey().topic(), entry.getKey().partition(), entry.getValue().longValue());
            }
            offsetInfoMap.put(entry.getKey(), new OffsetInfo(committedOffset, entry.getValue()));
        }
        return offsetInfoMap;
    }


}
