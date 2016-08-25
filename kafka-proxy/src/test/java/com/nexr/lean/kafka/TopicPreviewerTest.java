package com.nexr.lean.kafka;

import com.nexr.lean.kafka.util.TestServers;
import com.nexr.lean.kafka.util.Utils;
import com.nexr.lean.kafka.serde.AvroSerdeConfig;
import com.nexr.lean.kafka.serde.GenericAvroDeserializer;
import com.nexr.lean.kafka.serde.GenericAvroSerde;
import com.nexr.lean.kafka.util.SimpleKafakProducerExample;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

public class TopicPreviewerTest {

    private static Logger log = LoggerFactory.getLogger(TopicPreviewerTest.class);

    private static SimpleKafakProducerExample kafkaProducer = null;

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
    }

    @BeforeClass
    public static void setupClass() {
        try {
            setupEnvironment();
            if (testMethod.equals("unit-test")) {
                TestServers.startServers();
            }

            kafkaProducer = new SimpleKafakProducerExample(zkServers, brokers, schemaRegistryClass, schemaRegistryUrl);
            initData();

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

    public static void initData() {
        try {
            kafkaProducer.testSendTextMessage(20);
            kafkaProducer.testSendAvroID(20);
            kafkaProducer.testSendAvroMAGICBYTE_ID(20);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("fail to send data for consumer testing");
        }
    }

    @Test
    public void testPreviewText() {

        try {
            String topic = "az-text";
            int rowNumber = 10;

            TopicPreviewer previewer = new TopicPreviewer(brokers);
            List<ConsumerRecord<String, String>> lists = previewer.fetch(topic, 3000, rowNumber,
                    Utils.keyValueToProperties(
                            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
                    ), String.class);


            log.info("fetched size : {}", lists.size());
            Assert.assertTrue(lists.size() >= 10);

        } catch (Exception e) {
            Assert.fail("Fail to preview a topic of format is text");
        }

    }

    @Test
    public void testPreviewAvroID() throws InterruptedException {

        try {
            String topic = "az-avro-id";
            int rowNumber = 10;

            OffsetManager offsetManager = new OffsetManager(zkServers, brokers);
            Map<TopicPartition, Long> endOffsets = offsetManager.getEndOffset(topic);
            log.debug("--- endOffsets = {}", endOffsets.size());
            for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
                log.debug("topic={}, partition={}, offset={}", entry.getKey().topic(), entry.getKey().partition(),
                        entry.getValue().longValue());
            }

            TopicPreviewer previewer = new TopicPreviewer(brokers);
            Properties consumerProperties = getConsumerProperties();
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class.getName());
            consumerProperties.put(AvroSerdeConfig.HEADER_META_NAME_CONFIG, GenericAvroSerde.Meta.ID.name());
            List<ConsumerRecord<String, GenericRecord>> lists = previewer.fetch(topic, 3000, rowNumber,
                    consumerProperties, GenericRecord.class);

            log.info("fetched size : {}", lists.size());
            Assert.assertTrue(lists.size() >= 10);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Fail to preview a topic of format is text");
        } finally {
            Thread.sleep(500);
        }

    }

    @Test
    public void testPreviewAvroMagicByteID() {

        try {
            String topic = "az-avro-magicbyte-id";
            int rowNumber = 10;

            TopicPreviewer previewer = new TopicPreviewer(brokers);
            Properties consumerProperties = getConsumerProperties();
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class.getName());
            consumerProperties.put(AvroSerdeConfig.HEADER_META_NAME_CONFIG, GenericAvroSerde.Meta.MAGICBYTE_ID.name());
            List<ConsumerRecord<String, GenericRecord>> lists = previewer.fetch(topic, 3000, rowNumber,
                    consumerProperties, GenericRecord.class);


            log.info("fetched size : {}", lists.size());
            Assert.assertEquals(true, lists.size() >= 10);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Fail to preview a topic of format is text");
        }

    }

    @Test
    public void testParallel() throws Exception {
        final String topic = "az-text";
        final int rowNumber = 100;
        final TopicPreviewer previewer = new TopicPreviewer(brokers);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                List<ConsumerRecord<String, String>> list = previewer.fetch(topic, 3000, rowNumber,
                        Utils.keyValueToProperties(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
                        ), String.class);


                log.info("Thread={}, fetched size={}", Thread.currentThread().getName(), list.size());
                Assert.assertEquals(true, list.size() >= 4);

            }
        };

        Runnable runnable2 = new Runnable() {
            @Override
            public void run() {
                List<ConsumerRecord<String, GenericRecord>> list = previewer.fetch("az-avro-id", 2000, rowNumber,
                        Utils.keyValueToProperties(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class.getName(),
                                AvroSerdeConfig.HEADER_META_NAME_CONFIG, "ID",
                                AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass,
                                AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
                        ), GenericRecord.class);


                log.info("Thread={}, fetched size={}", Thread.currentThread().getName(), list.size());
                Assert.assertEquals(true, list.size() >= 4);

            }
        };

        Thread t1 = new Thread(runnable);
        t1.start();
        Thread t2 = new Thread(runnable);
        t2.start();
        Thread t3 = new Thread(runnable2);
        t3.start();

        Thread.sleep(7000);


    }

    private Properties getConsumerProperties() {
        return Utils.keyValueToProperties(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass,
                AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        );
    }

}
