package com.nexr.lean.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GetCommittedOffsetCallable<K, V> extends ConsumerCallable<K, List<ConsumerRecord<K, V>>, Map<TopicPartition, OffsetAndMetadata>> {

    private static Logger log = LoggerFactory.getLogger(GetCommittedOffsetCallable.class);

    private final Properties consumerProperties;

    public GetCommittedOffsetCallable(List<String> topics,
                                      Properties consumerProperties) {
        this("CommittedOffset", topics, consumerProperties);
    }

    public GetCommittedOffsetCallable(String id, List<String> topics,
                                      Properties consumerProperties) {
        super(id);
        this.topics = topics;
        this.groupId = consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        this.consumerProperties = consumerProperties;
        this.consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(consumerProperties);
    }

    @Override
    public void shutdown() {
        log.info("shutdown, wakeup consumer ");
        consumer.wakeup();
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> call() throws Exception {
        log.debug("call start. topic={} group={}", topics, groupId);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        try {
            consumer.subscribe(topics);

            offsets.putAll(findLastCommittedOffset());

            log.info("after get lastCommitted. groupId={}, tp size={}", groupId, offsets.size());

        } catch (WakeupException e) {
            // do nothing for this exception.
        } catch (Exception e) {
            log.warn("Fail to get committed offset ", e);
        } finally {
            consumer.close();
        }
        log.debug(" call end ");
        return offsets;
    }

}
