package com.nexr.lean.kafka.util;

import com.nexr.lean.kafka.KafkaProxyTestServers;
import com.nexr.lean.kafka.common.ConfigUtils;
import com.nexr.lean.kafka.serde.AvroSerdeConfig;
import com.nexr.lean.kafka.serde.GenericAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Future;

public class SimpleKafkaConsumerTest {

    private static Logger log = LoggerFactory.getLogger(SimpleKafkaConsumerTest.class);

    private static SimpleKafakProducerExample kafkaProducer = null;

    private static String zkServers = null;
    private static String brokers = null;
    private static String schemaRegistryClass = null;
    private static String schemaRegistryUrl = null;

    public static void setupEnvironment() {
        zkServers = "localhost:" + KafkaProxyTestServers.ZK_PORT;
        brokers = "localhost:" + KafkaProxyTestServers.BROKER_PORT;
        schemaRegistryClass = "com.nexr.lean.kafka.util.DummySchemaRegistryClient";
        schemaRegistryUrl = "http://hello:18181/repo";
    }

    @BeforeClass
    public static void setupClass() {
        try {
            setupEnvironment();
            kafkaProducer = new SimpleKafakProducerExample(zkServers, brokers, schemaRegistryClass, schemaRegistryUrl);

            KafkaProxyTestServers.startServers();

            initData();
        } catch (Exception e) {
            log.warn("Fail to initialize the local kafka, local zookeeper for testing");
            Assert.fail();
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            KafkaProxyTestServers.shutdownServers();
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
    public void testConsumeTextSync() {
        String topic = "az-text";
        String groupId = "az-group";

        SimpleKafkaConsumer kafkaConsumer = new SimpleKafkaConsumer(brokers);

        List<ConsumerRecord<String, String>> datas = kafkaConsumer.fetchSync(topic, 2000, 10,
                ConfigUtils.keyValueToProperties(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
                ), String.class);
        log.info(" fetchSynch {}, data size {}", topic, datas.size());
        for (ConsumerRecord<String, String> record : datas) {
            log.info("record : {}", record.value().toString());
        }
        log.info(" fetchSynch end");
        Assert.assertTrue(datas.size() >= 10);

        try {
            Thread.sleep(1000);
            kafkaConsumer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConsumeTextASync() {
        String topic = "az-text";
        String groupId = "az-group";

        SimpleKafkaConsumer kafkaConsumer = new SimpleKafkaConsumer(brokers);

        Future<List<ConsumerRecord<String, String>>> future = kafkaConsumer.fetchAsync(topic, 3000, 10,
                ConfigUtils.keyValueToProperties(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
                ),
                new SimpleKafkaConsumer.FetchCallback<String, String>() {
                    @Override
                    public void onComplete(List<? extends ConsumerRecord<String, String>> consumerRecords, Exception e) {
                        log.info(" fetchAynch text format, data size {}", consumerRecords.size());
                        for (ConsumerRecord<String, String> record : consumerRecords) {
                            log.info("record : {}", record.value());
                        }
                    }
                });

        try {
            // only for testing.
            List list = future.get();
            Assert.assertEquals(true, list.size() >= 10);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Fail to fetch asyn from text format topic");
        }

        try {
            Thread.sleep(1000);
            kafkaConsumer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info(" fetchASynch end");
    }

    @Test
    public void testConsumeAvroIDSync() {
        String topic = "az-avro-id";
        String groupId = "az-group";

        SimpleKafkaConsumer kafkaConsumer = new SimpleKafkaConsumer(brokers);

        List<ConsumerRecord<String, GenericRecord>> list = kafkaConsumer.fetchSync(topic, 2000, 10,
                ConfigUtils.keyValueToProperties(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class.getName(),
                        AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass,
                        AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        AvroSerdeConfig.HEADER_META_NAME_CONFIG, "ID"
                ), GenericRecord.class);
        for (ConsumerRecord<String, GenericRecord> record : list) {
            log.info("record : {}", record.value().toString());
        }
        log.info("records size : {},", list.size());
        Assert.assertTrue(list.size() >= 10);

        try {
            Thread.sleep(1000);
            kafkaConsumer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConsumeAvroIDAsync() {
        String topic = "az-avro-id";
        String groupId = "az-group";

        SimpleKafkaConsumer simpleKafkaConsumer = new SimpleKafkaConsumer(brokers);

        long timeout = 2000;
        int rowNumber = 10;

        Future<List<ConsumerRecord<String, GenericRecord>>> listFuture = simpleKafkaConsumer.fetchAsync(topic, timeout, rowNumber,
                ConfigUtils.keyValueToProperties(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class.getName(),
                        AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass,
                        AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        AvroSerdeConfig.HEADER_META_NAME_CONFIG, "ID"
                ),
                new SimpleKafkaConsumer.FetchCallback<String, GenericRecord>() {
                    @Override
                    public void onComplete(List<? extends ConsumerRecord<String, GenericRecord>> consumerRecords, Exception e) {
                        log.info("---- fetch complete : size {} ", consumerRecords.size());
                    }
                }
        );

        log.info("--------- testFetchConsumerAvroID before sleep ......");

        try {
            Thread.sleep(1000);
            simpleKafkaConsumer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("--------- testFetchConsumerAvroID after sleep ......");

    }


    @Test
    public void testConsumeAvroMagicByteIDSync() {

        String topic = "az-avro-magicbyte-id";
        String groupId = "az-group";

        SimpleKafkaConsumer kafkaConsumer = new SimpleKafkaConsumer(brokers);

        List<ConsumerRecord<String, GenericRecord>> list = kafkaConsumer.fetchSync(topic, 4000, 10,
                ConfigUtils.keyValueToProperties(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class.getName(),
                        AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass,
                        AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        AvroSerdeConfig.HEADER_META_NAME_CONFIG, "MAGICBYTE_ID"
                ), GenericRecord.class);
        for (ConsumerRecord<String, GenericRecord> record : list) {
            log.info("record : {}", record.value().toString());
        }
        log.info("Data size from avro-magicbyte-id : {}", list.size());
        Assert.assertTrue(list.size() >= 10);

        try {
            Thread.sleep(1000);
            kafkaConsumer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
