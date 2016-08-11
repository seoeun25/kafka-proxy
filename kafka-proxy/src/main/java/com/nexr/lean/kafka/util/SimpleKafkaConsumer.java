package com.nexr.lean.kafka.util;

import com.google.common.collect.ImmutableList;
import com.nexr.lean.kafka.common.KafkaProxyRuntimeException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SimpleKafkaConsumer {

    public static final int THREAD_NUM = 10; // TODO make configurable
    private static Logger log = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
    private final String brokers;
    private final ExecutorService executorService;

    public SimpleKafkaConsumer(String brokers) {
        this.brokers = brokers;
        this.executorService = Executors.newFixedThreadPool(THREAD_NUM);
    }

    /**
     * Fetch data for the topics.
     *
     * @param timeout   the time spent waiting if data is not available. In milliseconds.
     * @param rowNumber the max number of fetch data.
     * @return the list of data retrieved
     */
    public <V> List<ConsumerRecord<String, V>> fetchSync(String topic, long timeout, int rowNumber, Properties
            consumerProperties, Class<V> valueClass) {

        Future<List<ConsumerRecord<String, V>>> future = fetchAsync(topic, timeout, rowNumber, consumerProperties, null);

        try {
            return future.get();
        } catch (Exception e) {
            throw new KafkaProxyRuntimeException(e);
        }
    }

    /**
     * Fetch data for the topics.
     *
     * @param timeout   the time spent waiting if data is not available. In milliseconds. It will be great if this time include
     *                  the initialize time for  kafka consumer.
     * @param rowNumber the max number of fetch data.
     * @return the Future of the fetchtask
     */
    public <V> Future<List<ConsumerRecord<String, V>>> fetchAsync(String topic, long timeout, int rowNumber,
                                                                  Properties consumerProperties,
                                                                  final FetchCallback<String, V> callback) {
        if (timeout < 0) {
            throw new IllegalArgumentException("Timemout must not be negative");
        }
        List<String> topics = ImmutableList.of(topic);

        long taskTimeout = (timeout - 500) < 0 ? 100 : timeout - 500;

        FetchCallable<String, V> previewTask = new FetchCallable<>(String.valueOf(0), taskTimeout, rowNumber, topics,
                consumerProperties, callback
        );

        return executorService.submit(previewTask);
    }

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
