package com.nexr.lean.kafka.consumer;

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
public class FetchTask<K, V> extends ConsumerTask<K, List<ConsumerRecord<K, V>>, List<ConsumerRecord<K, V>>> {

    private static Logger log = LoggerFactory.getLogger(FetchTask.class);

    private final Properties consumerProperties;
    private long timeout = 2000; // TODO global configurable
    private int minRowNumber;  // TODO global configurable
    private boolean manualCommit;
    private List<ConsumerRecord<K, V>> fetchedDatas;
    private ConsumerService.FetchCallback<K, V> callback;

    public FetchTask(long timeout, int minRowNumber, List<String> topics,
                     Properties consumerProperties, ConsumerService.FetchCallback<K, V> callback) {
        this("Fetch", timeout, minRowNumber, topics, consumerProperties,
                callback);
    }


    public FetchTask(String id, long timeout, int minRowNumber, List<String> topics,
                     Properties consumerProperties, ConsumerService.FetchCallback<K, V> callback) {
        super(id);
        this.timeout = timeout;
        this.topics = topics;
        this.groupId = consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        this.minRowNumber = minRowNumber;
        this.manualCommit = new Boolean(consumerProperties.getProperty(SimpleConsumerConfig.ENABLE_MANUAL_COMMIT_CONFIG, "false")).booleanValue();
        this.fetchedDatas = new ArrayList<>();
        this.consumerProperties = consumerProperties;
        this.consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.callback = callback;
        this.consumer = new KafkaConsumer<>(consumerProperties);
    }

    private void consumeRecrods(ConsumerRecords<K, V> records) {

        for (ConsumerRecord<K, V> record : records) {
            log.trace("partition={}, offset={}, value={}",
                    record.partition(), record.offset(), record.value());
            fetchedDatas.add(record);
        }
    }


    @Override
    public void shutdown() {
        log.info("shutdown, wakeup consumer ");
        consumer.wakeup();
    }

    @Override
    public List<ConsumerRecord<K, V>> call() throws Exception {
        long expireTime = System.currentTimeMillis() + timeout;
        log.debug("call start. topic={} group={} timeout={}", topics, groupId, timeout);
        try {
            consumer.subscribe(topics);

            findLastCommittedOffset();

            int retry = 0;
            while (true) {
                ConsumerRecords<K, V> records = (ConsumerRecords<K, V>) consumer.poll(timeout);
                log.debug("polling records count={} ", records.count());
                consumeRecrods(records);
                if (System.currentTimeMillis() >= expireTime || fetchedDatas.size() >= minRowNumber) {
                    if (records.count() == 0 && retry < 3) {
                        retry++;
                        log.debug("retry = {} ", retry);
                    } else {
                        break;
                    }
                }
            }

            log.debug("finish poll, fetchDatas={}", fetchedDatas.size());
            if (callback != null) {
                callback.onComplete(fetchedDatas, null);
            }

            if (manualCommit) {
                commitOffset();
            }
        } catch (WakeupException e) {
            // do nothing for exception.
            if (callback != null) {
                callback.onComplete(fetchedDatas, null);
            }
        } catch (Exception e) {
            log.warn("Fail to fetch ", e);
            if (callback != null) {
                callback.onComplete(null, e);
            }
        } finally {
            consumer.close();
        }
        return fetchedDatas;
    }

}
