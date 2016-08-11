package com.nexr.lean.kafka.util;

import com.nexr.lean.kafka.common.KafkaProxyRuntimeException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public abstract class ConsumerCallable<K, V, T> implements Callable<T> {

    private static Logger log = LoggerFactory.getLogger(ConsumerCallable.class);

    protected final String id;
    protected final boolean manualCommit;
    protected KafkaConsumer<K, V> consumer;
    protected List<String> topics;

    public ConsumerCallable(String id, boolean manualCommit) {
        this.id = id;
        this.manualCommit = manualCommit;
    }

    public abstract void shutdown();

    @Override
    public abstract T call() throws Exception;

    /**
     * Find the last commited offset
     */
    public boolean findLastCommitedOffset() {
        String topic = topics.get(0);
        List<PartitionInfo> partitionInfos = consumer.listTopics().get(topic);
        if (partitionInfos == null) {
            log.debug("No partition infos");
            return false;
        }
        boolean find = true;
        for (PartitionInfo partitionInfo : partitionInfos) {
            OffsetAndMetadata offsetAndMetadata = consumer.committed(new TopicPartition(topic, partitionInfo.partition()));
            if (offsetAndMetadata == null) {
                log.debug("No OffsetAndMetadata of {}, {}", topic, partitionInfo.partition());
                find = false;
            } else {
                log.debug("Last committed offset of partition {}, {} : {}", topic, partitionInfo.partition(),
                        offsetAndMetadata.toString());
            }
        }
        return find;
    }

    public void findEndOffset() {
        Collection<TopicPartition> assigned = consumer.assignment();
        consumer.seekToEnd(assigned);
        for (TopicPartition topicPartition : assigned) {
            long position = consumer.position(topicPartition);
            log.debug("end offset: partition={}, position={}", topicPartition.partition(), position);
        }
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
        ConsumerRecords<K, V> records = (ConsumerRecords<K, V>) consumer.poll(10);
        log.debug(" consumer {} , pollOnce: records={}", id, records.count());
    }

    /**
     * Call this after poll()
     */
    public void commitOffset() {
        if (!manualCommit) {
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

    public void commitOffset(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (!manualCommit) {
            consumer.commitSync(offsets);
        }
    }


}
