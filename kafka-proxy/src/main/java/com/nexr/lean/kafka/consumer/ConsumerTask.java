package com.nexr.lean.kafka.consumer;

import com.nexr.lean.kafka.common.KafkaProxyRuntimeException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

public abstract class ConsumerTask<K, V, T> implements Callable<T> {

    private static Logger log = LoggerFactory.getLogger(ConsumerTask.class);

    protected final String id;
    protected KafkaConsumer<K, V> consumer;
    protected List<String> topics;
    protected String groupId;

    public ConsumerTask(String id) {
        this.id = id;
    }

    public abstract void shutdown();

    @Override
    public abstract T call() throws Exception;

    /**
     * Search the committed offset
     *
     * @return a map that contains <code>OffsetAndMeta</code> for each partition.
     * <p> If there is no topic, return empty list.</p>
     * <p> If there is no offset, the value in the map could be null.</p>
     */
    public Map<TopicPartition, OffsetAndMetadata> findLastCommittedOffset() {
        String topic = topics.get(0);
        List<PartitionInfo> partitionInfos = consumer.listTopics().get(topic);
        if (partitionInfos == null) {
            log.debug("No partition info");
            return new HashMap<>();
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            OffsetAndMetadata offsetAndMetadata = consumer.committed(new TopicPartition(topic, partitionInfo.partition()));
            offsets.put(new TopicPartition(topic, partitionInfo.partition()), offsetAndMetadata);
        }
        return offsets;
    }

    public Map<TopicPartition, Long> findEndOffset() {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        Collection<TopicPartition> assigned = consumer.assignment();
        consumer.seekToEnd(assigned);
        for (TopicPartition topicPartition : assigned) {
            long position = consumer.position(topicPartition);
            offsets.put(topicPartition, position);
        }
        return offsets;
    }

    public List<TopicPartition> getPartitionInfos(String topic) {
        List<PartitionInfo> partitionInfos = consumer.listTopics().get(topic);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
        }
        return topicPartitions;
    }

    public void pollOnce() {
        ConsumerRecords<K, V> records = (ConsumerRecords<K, V>) consumer.poll(0);
        log.trace("pollOnce: records={}", records.count());
    }

    /**
     * Call this after poll()
     */
    public void commitOffset() {
        log.debug("commitOffset");
        consumer.commitAsync(new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        log.warn("Commit offset fail: {}, {}", entry.getValue().metadata(), entry.getValue().offset());
                    }
                    throw new KafkaProxyRuntimeException("Fail to commit offset", exception);
                } else {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        log.debug("Commit offset : {}, {}", entry.getValue().metadata(), entry.getValue().offset());
                    }
                }
            }
        });
    }

}
