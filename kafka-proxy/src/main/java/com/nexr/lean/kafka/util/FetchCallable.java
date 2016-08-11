package com.nexr.lean.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Fetch from topic. After fetch, it does not commit offset.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class FetchCallable<K, V> extends ConsumerCallable<K, List<ConsumerRecord<K, V>>, List<ConsumerRecord<K, V>>> {

    private static Logger log = LoggerFactory.getLogger(FetchCallable.class);

    private final Properties consumerProperties;
    private long timeout = 2000;
    private int rowNumber;  // configurable
    private List<ConsumerRecord<K, V>> datas;
    private SimpleKafkaConsumer.FetchCallback<K, V> callback;

    public FetchCallable(String id, long timeout, int rowNumber, List<String> topics,
                         Properties consumerProperties, SimpleKafkaConsumer.FetchCallback<K, V> callback) {
        super(id, false);
        this.timeout = timeout;
        this.topics = topics;
        this.rowNumber = rowNumber;
        this.datas = new ArrayList<>(rowNumber);
        this.consumerProperties = consumerProperties;
        this.consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.callback = callback;
        this.consumer = new KafkaConsumer<>(consumerProperties);
    }

    private void consumeRecrods(ConsumerRecords<K, V> records) {

        for (ConsumerRecord<K, V> record : records) {
            if (datas.size() >= rowNumber) {
                break;
            }
            log.trace("consumer[{}] : partition={}, offset={}, value={}", this.id,
                    record.partition(), record.offset(), record.value());
            datas.add(record);
        }
    }


    @Override
    public void shutdown() {
        log.info("Consumer {} shutdowning, wakeup consumer ", id);
        consumer.wakeup();
    }

    @Override
    public List<ConsumerRecord<K, V>> call() throws Exception {
        long expireTime = System.currentTimeMillis() + timeout;
        try {
            consumer.subscribe(topics);

            findLastCommitedOffset();

            while (true) {
                ConsumerRecords<K, V> records = (ConsumerRecords<K, V>) consumer.poll(timeout);
                log.debug(" consumer {} , polling records count {} ", id, records.count());
                consumeRecrods(records);
                if (System.currentTimeMillis() >= expireTime || datas.size() >= rowNumber) {
                    break;
                }
            }

            log.debug("finish poll : " + datas.size());
            if (callback != null) {
                callback.onComplete(datas, null);
            }

            if (manualCommit) {
                log.debug("manual offset commit");
                commitOffset();
            }
        } catch (WakeupException e) {
            // do nothing for exception.
            if (callback != null) {
                callback.onComplete(datas, null);
            }
        } catch (Exception e) {
            log.warn("Fail to poll, consumer=" + id, e);
            if (callback != null) {
                callback.onComplete(null, e);
            }
        } finally {
            consumer.close();
        }
        return datas;
    }

}
