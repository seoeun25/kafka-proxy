package com.nexr.lean.kafka;

import com.nexr.lean.kafka.common.OffsetInfo;
import com.nexr.lean.kafka.util.TestServers;
import com.nexr.lean.kafka.util.Utils;
import com.nexr.lean.kafka.consumer.SimpleConsumerConfig;
import com.nexr.lean.kafka.util.SimpleKafakProducerExample;
import com.nexr.lean.kafka.consumer.ConsumerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OffsetManagerTest {

    private static Logger log = LoggerFactory.getLogger(TopicPreviewerTest.class);

    private static SimpleKafakProducerExample kafkaProducer = null;
    private static OffsetManager offsetManager = null;
    private static ConsumerService consumerService = ConsumerService.getInstance();
    private static String testMethod = null;
    private static String zkServers = null;
    private static String brokers = null;
    private static String schemaRegistryClass = null;
    private static String schemaRegistryUrl = null;

    public static void setupEnvironment() {
        Properties properties = TestServers.getPropertiesForTesting();
        testMethod = properties.getProperty("test.method");
        zkServers = properties.getProperty("zkServers");
        brokers = properties.getProperty("brokers");
        schemaRegistryClass = properties.getProperty("schemaRegistryClass");
        schemaRegistryUrl = properties.getProperty("schemaRegistryUrl");
        log.debug("setupEnvironment : zkServer={}, brokers={}, schemaRegistryClass={}, schemaRegistryUrl={}", zkServers, brokers,
                schemaRegistryClass, schemaRegistryUrl);
    }

    @BeforeClass
    public static void setupClass() {
        try {
            setupEnvironment();
            if (testMethod.equals("unit-test")) {
                TestServers.startServers();
            }

            kafkaProducer = new SimpleKafakProducerExample(zkServers, brokers, schemaRegistryClass, schemaRegistryUrl);

            offsetManager = new OffsetManager(zkServers, brokers);
        } catch (Exception e) {
            log.warn("Fail to initialize the local kafka, local zookeeper for testing");
            Assert.fail();
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            TestServers.shutdownServers();
        } catch (Exception e) {
            log.warn("Fail to shutdown the local kafka, local zookeeper for testing");
        }
    }

    @Test
    public void getLastCommittedOffsets() throws InterruptedException {
        String topic = "az-text-offset";
        String groupId = "i-am-consumer";

        // no topics. no partition infos.
        Map<TopicPartition, OffsetAndMetadata> offsets0 = null;
        try {
            offsetManager.getCommittedOffset(topic, groupId);
            Assert.fail("It should throw Exception about topic not found");
        } catch (Exception e) {

        }

        if (!TopicManager.topicExists(zkServers, topic)) {
            TopicManager.createTopic(zkServers, topic, 2, 1);
        }

        try {
            kafkaProducer.testSendTextMessage(topic, 10);
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("fail to produce messages for testing");
        }

        // after produce, partitions info exists, no committed offsets
        offsets0 = offsetManager.getCommittedOffset(topic, groupId);
        Assert.assertEquals(true, offsets0.size() > 0);

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets0.entrySet()) {
            log.debug("[{}-{}] : 00 offset-={}", entry.getKey().topic(), entry.getKey().partition(),
                    entry.getValue() == null ? null : entry.getValue().offset());
        }

        Properties consumerProperties = Utils.keyValueToProperties(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                SimpleConsumerConfig.ENABLE_MANUAL_COMMIT_CONFIG, "true"
        );

        List datas = consumerService.fetchSync(topic, 3000, 10,
                consumerProperties, String.class);
        log.debug("fetch data size={}", datas.size());

        Map<TopicPartition, OffsetAndMetadata> offsets1 = offsetManager.getCommittedOffset(topic, groupId);

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets1.entrySet()) {
            log.debug("[{}-{}] : 00 offset-={}", entry.getKey().topic(), entry.getKey().partition(),
                    entry.getValue() == null ? null : entry.getValue().offset());
        }

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets0.entrySet()) {
            OffsetAndMetadata old = entry.getValue();
            OffsetAndMetadata committed = offsets1.get(entry.getKey());
            log.debug(" partition={}, offset old {} ", entry.getKey().partition(), old);
            log.debug(" partition={}, offset commited {} ", entry.getKey().partition(), committed);
            Assert.assertEquals(true, ((old == null ? 0 : old.offset()) < committed.offset()));
        }

    }

    @Test
    public void getEndOffsets() throws InterruptedException {
        final String topic = "az-text-end";
        String groupId = "i-am-fetcher";

        try {
            Map<TopicPartition, Long> a0 = offsetManager.getEndOffset(topic);
        } catch (Exception e) {
            Assert.assertEquals(true, e.getMessage().contains("not found"));
        }

        Thread sender = new Thread(new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        kafkaProducer.testSendTextMessage(topic, 10000, 30);
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        break;
                    } catch (Exception e) {
                        break;
                    }
                }
            }
        });
        sender.start();

        Thread.sleep(500);

        // produce logs. no committed offset.
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = offsetManager.getCommittedOffset(topic, groupId);
        Assert.assertEquals(true, committedOffsets.size() > 0);
        Map<TopicPartition, Long> endOffsets = offsetManager.getEndOffset(topic);
        log.debug("endoffsets : size={}", endOffsets.size());
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            Assert.assertEquals(true, committedOffsets.containsKey(entry.getKey()));
            log.debug("[{}-{}] before committed={}, end={}", entry.getKey().topic(), entry.getKey().partition(),
                    committedOffsets.get(entry.getKey()), entry.getValue().longValue());
        }

        Properties consumerProperties = Utils.keyValueToProperties(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                SimpleConsumerConfig.ENABLE_MANUAL_COMMIT_CONFIG, "true"
        );

        List datas = consumerService.fetchSync(topic, 2000, 10,
                consumerProperties, String.class);
        log.debug("fetch data size={}", datas.size());

        committedOffsets = offsetManager.getCommittedOffset(topic, groupId);

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committedOffsets.entrySet()) {
            log.debug("[{}-{}] : committed offset-={}", entry.getKey().topic(), entry.getKey().partition(),
                    entry.getValue() == null ? null : entry.getValue().offset());
        }

        Thread.sleep(500);
        endOffsets = offsetManager.getEndOffset(topic);
        log.debug("endoffsets : size={}", endOffsets.size());
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            Assert.assertEquals(true, committedOffsets.containsKey(entry.getKey()));
            log.debug("[{}-{}] committed={}, end={}", entry.getKey().topic(), entry.getKey().partition(),
                    committedOffsets.get(entry.getKey()).offset(), entry.getValue().longValue());
            Assert.assertEquals(true, entry.getValue().longValue() > committedOffsets.get(entry.getKey()).offset());

        }

        sender.interrupt();
        Thread.sleep(500);

    }

    @Test
    public void getOffsetInfoTest() throws InterruptedException {
        final String topic = "az-text-offsetinfo";
        String groupId = "i-am-fetcher";

        // no topic
        try {
            Map<TopicPartition, OffsetInfo> a0 = offsetManager.getOffsets(topic, groupId);
        } catch (Exception e) {
            Assert.assertEquals(true, e.getMessage().contains("not found"));
        }

        if (!TopicManager.topicExists(zkServers, topic)) {
            TopicManager.createTopic(zkServers, topic, 2, 1);
        }
        Thread.sleep(100);

        // topic exists. no log
        Map<TopicPartition, OffsetInfo> offsetInfoMap = offsetManager.getOffsets(topic, groupId);
        Assert.assertEquals(true, offsetInfoMap.size() > 0);
        log.info("offsetInfoMap tp size={}", offsetInfoMap.size());
        for (Map.Entry<TopicPartition, OffsetInfo> entry : offsetInfoMap.entrySet()) {
            log.info("topic={}, partition={}, committed={}, end={}", entry.getKey().topic(), entry.getKey().partition(),
                    entry.getValue().getCommitted(), entry.getValue().getEnd());
            Assert.assertEquals(-1, entry.getValue().getCommitted());
        }

        Thread sender = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        kafkaProducer.testSendTextMessage(topic, 10000, 50);
                    } catch (InterruptedException e) {
                        break;
                    } catch (Exception e) {
                        break;
                    }
                }
            }
        });
        sender.start();

        Thread.sleep(500);

        // produce logs. no committed offset.
        offsetInfoMap = offsetManager.getOffsets(topic, groupId);
        Assert.assertEquals(true, offsetInfoMap.size() > 0);
        log.info("offsetInfoMap tp size={}", offsetInfoMap.size());
        for (Map.Entry<TopicPartition, OffsetInfo> entry : offsetInfoMap.entrySet()) {
            log.info("topic={}, partition={}, committed={}, end={}", entry.getKey().topic(), entry.getKey().partition(),
                    entry.getValue().getCommitted(), entry.getValue().getEnd());
            Assert.assertEquals(-1, entry.getValue().getCommitted());
            Assert.assertEquals(true, entry.getValue().getEnd() > 0);
            Assert.assertEquals(true, entry.getValue().getCommitted() <= entry.getValue().getEnd());
        }

        // fetch --> committed offset
        Properties consumerProperties = Utils.keyValueToProperties(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                SimpleConsumerConfig.ENABLE_MANUAL_COMMIT_CONFIG, "true"
        );

        List datas = consumerService.fetchSync(topic, 3000, 10,
                consumerProperties, String.class);
        log.debug("fetch data size={}", datas.size());

        // produce logs. committed offset.
        offsetInfoMap = offsetManager.getOffsets(topic, groupId);
        Assert.assertEquals(true, offsetInfoMap.size() > 0);
        log.info("offsetInfoMap tp size={}", offsetInfoMap.size());
        for (Map.Entry<TopicPartition, OffsetInfo> entry : offsetInfoMap.entrySet()) {
            log.info("topic={}, partition={}, committed={}, end={}", entry.getKey().topic(), entry.getKey().partition(),
                    entry.getValue().getCommitted(), entry.getValue().getEnd());
            Assert.assertEquals(true, entry.getValue().getCommitted() > -1);
            Assert.assertEquals(true, entry.getValue().getEnd() > 0);
            Assert.assertEquals(true, entry.getValue().getCommitted() <= entry.getValue().getEnd());
        }

        sender.interrupt();
        Thread.sleep(500);

    }

}
