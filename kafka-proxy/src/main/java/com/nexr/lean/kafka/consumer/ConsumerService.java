package com.nexr.lean.kafka.consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.nexr.lean.kafka.common.KafkaProxyRuntimeException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConsumerService {

    public static final int THREAD_NUM = 10; // TODO make configurable
    private static Logger log = LoggerFactory.getLogger(ConsumerService.class);
    private final ExecutorService executorService;

    private static volatile ConsumerService instance;

    // TODO : Use injection.
    public static ConsumerService getInstance() {
        if (instance == null) {
            instance = new ConsumerService();
        }
        return instance;
    }

    private ConsumerService() {
        this.executorService = Executors.newFixedThreadPool(THREAD_NUM,
                new ThreadFactoryBuilder().setNameFormat("consumer-pool-%d").build());
    }

    public <V> List<ConsumerRecord<String, V>> fetchSync(String topic, long timeout, int minRowNumber, Properties
            consumerProperties, Class<V> valueClass) {

        Future<List<ConsumerRecord<String, V>>> future = fetchAsync(topic, timeout, minRowNumber, consumerProperties, null);

        try {
            return future.get();
        } catch (Exception e) {
            throw new KafkaProxyRuntimeException(e);
        }
    }

    /**
     * Fetch data for the topics.
     *
     * @param timeout      the time spent waiting if data is not available. In milliseconds. It will be great if this time include
     *                     the initialize time for  kafka consumer.
     * @param minRowNumber the number of fetch data approximately.
     * @return the Future of the fetchtask. If there are lots of messages in topic, the fetch data could be
     * greater than rowNumber. Otherwise, If there are messages fewer than rowNumber, the fetch data contain the
     * all message but size is smaller than rowNumber.
     */
    public <V> Future<List<ConsumerRecord<String, V>>> fetchAsync(String topic, long timeout, int minRowNumber,
                                                                  Properties consumerProperties,
                                                                  final FetchCallback<String, V> callback) {
        if (timeout < 0) {
            throw new IllegalArgumentException("Timemout must not be negative");
        }
        List<String> topics = ImmutableList.of(topic);

        long taskTimeout = (timeout - 500) < 0 ? 100 : timeout - 500;

        FetchTask<String, V> previewTask = new FetchTask<>(taskTimeout, minRowNumber, topics,
                consumerProperties, callback
        );

        return executorService.submit(previewTask);
    }

    /**
     * Gets the last committed offsets for a {@code topic} and {@code groupId}
     *
     * @param topic              the topic to check
     * @param groupId            the group of consumer
     * @param consumerProperties the properties for the consumer
     * @return the last committed offsets
     */
    public Map<TopicPartition, OffsetAndMetadata> getCommittedOffset(String topic, String groupId, Properties consumerProperties) {
        List<String> topics = ImmutableList.of(topic);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        GetCommittedOffsetTask getOffsetCallable = new GetCommittedOffsetTask<>(topics, consumerProperties);

        Future result = executorService.submit(getOffsetCallable);
        try {
            return (Map<TopicPartition, OffsetAndMetadata>) result.get();
        } catch (Exception e) {
            throw new KafkaProxyRuntimeException(e);
        }
    }

    public Map<TopicPartition, Long> getEndOffset(String topic, String groupId, Properties consumerProperties) {
        List<String> topics = ImmutableList.of(topic);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        GetEndOffsetTask getEndOffsetCallable = new GetEndOffsetTask(topics, consumerProperties);
        try {
            Future future = executorService.submit(getEndOffsetCallable);
            return (Map<TopicPartition, Long>) future.get();
        } catch (Exception e) {
            throw new KafkaProxyRuntimeException(e);
        }
    }

    // TODO who should call?. Still no application main.
    public void shutdown() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn("Fail to shutdown", e);
        }
    }

    public interface FetchCallback<K, V> {
        void onComplete(List<? extends ConsumerRecord<K, V>> records, Exception e);
    }
}
