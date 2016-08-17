package com.nexr.lean.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GetEndOffsetCallable<K, V> extends ConsumerCallable<K, List<ConsumerRecord<K, V>>, Map<TopicPartition, Long>> {

    private static Logger log = LoggerFactory.getLogger(GetEndOffsetCallable.class);

    private final Properties consumerProperties;

    public GetEndOffsetCallable(List<String> topics,
                                Properties consumerProperties) {
        this("EndOffset", topics, consumerProperties);
    }

    public GetEndOffsetCallable(String id, List<String> topics,
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
    public Map<TopicPartition, Long> call() throws Exception {
        log.debug("call start. topic={} group={}", topics, groupId);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        try {
            consumer.subscribe(topics);

            pollOnce();
            findEndOffset();

            offsets.putAll(findEndOffset());

            log.debug("after find EndOffset. groupId={}, tp size={}", groupId, offsets.size());

        } catch (WakeupException e) {
            // do nothing for this exception.
        } catch (Exception e) {
            log.warn("Fail to get end offset ", e);
        } finally {
            consumer.close();
        }
        log.debug(" call end ");
        return offsets;
    }

}
