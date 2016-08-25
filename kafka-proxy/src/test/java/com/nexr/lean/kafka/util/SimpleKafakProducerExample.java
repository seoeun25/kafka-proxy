package com.nexr.lean.kafka.util;

import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;
import com.nexr.lean.kafka.TopicManager;
import com.nexr.lean.kafka.serde.AvroSerdeConfig;
import com.nexr.lean.kafka.serde.CachedSchemaRegistryTest;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleKafakProducerExample {

    private static Logger log = LoggerFactory.getLogger(SimpleKafakProducerExample.class);

    private String zkServers = null;
    private String brokers = null;
    private String schemaRegistryClass = null;
    private String schemaRegistryUrl = null;

    public SimpleKafakProducerExample(String zkServers, String brokers, String schemaRegistryClass, String schemaRegistryUrl) {
        this.zkServers = zkServers;
        this.brokers = brokers;
        this.schemaRegistryClass = schemaRegistryClass;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public static GenericRecord createEmployeeRecord(String topic) {
        Schema employeeSchema = new Schema.Parser().parse(CachedSchemaRegistryTest.employee_schema_test);

        Schema headerSchema = employeeSchema.getField("header").schema();

        GenericRecord record = new GenericData.Record(employeeSchema);
        record.put("name", "seoeun");
        record.put("favorite_number", String.valueOf(9));
        record.put("wrk_dt", System.currentTimeMillis());
        record.put("src_info", topic);
        GenericRecord header = new GenericData.Record(headerSchema);
        header.put("time", record.get("wrk_dt"));
        record.put("header", header);
        return record;
    }

    private Properties getProducerProperties() {
        return Utils.keyValueToProperties(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                "schema.registry.url", "http://localhost:18181/repo",
                KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS, "com.nexr.schemaregistry.AvroSchemaRegistry"
        );
    }

    public void testSendTextMessage(int rowNumber) throws Exception {
        String topic = "az-text";
        if (!TopicManager.topicExists(zkServers, brokers)) {
            TopicManager.createTopic(zkServers, topic, 2, 1);
        }

        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer(SimpleKafkaProducer.PRODUCER_TYPE
                .text, getProducerProperties());

        for (int i = 0; i < rowNumber; i++) {
            Object message = String.valueOf(i) + "local-aaa-new-seoeun-new--" + String.valueOf(i);
            simpleKafkaProducer.send(topic, message);

            Thread.sleep(5);
        }
        log.debug("end send ");
    }

    public void testSendAvroID(int rowNumber) throws Exception {
        String topic = "az-avro-id";
        if (!TopicManager.topicExists(zkServers, brokers)) {
            TopicManager.createTopic(zkServers, topic, 2, 1);
        }

        Properties producerProperties = getProducerProperties();
        producerProperties.setProperty(AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass);
        producerProperties.setProperty(AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        producerProperties.setProperty(AvroSerdeConfig.HEADER_META_NAME_CONFIG, "ID");

        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer(SimpleKafkaProducer.PRODUCER_TYPE.avro,
                producerProperties);

        for (int i = 0; i < rowNumber; i++) {
            GenericRecord record = createEmployeeRecord(topic);
            record.put("name", String.valueOf(i) + "::" + record.get("name"));
            simpleKafkaProducer.send(topic, record);

            Thread.sleep(5);
        }
    }

    public void testSendAvroMAGICBYTE_ID(int rowNumber) throws Exception {
        String topic = "az-avro-magicbyte-id";
        if (!TopicManager.topicExists(zkServers, brokers)) {
            TopicManager.createTopic(zkServers, topic, 2, 1);
        }

        Properties producerProperties = getProducerProperties();
        producerProperties.setProperty(AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass);
        producerProperties.setProperty(AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        producerProperties.setProperty(AvroSerdeConfig.HEADER_META_NAME_CONFIG, "MAGICBYTE_ID");

        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer(SimpleKafkaProducer.PRODUCER_TYPE.avro,
                producerProperties);

        for (int i = 0; i < rowNumber; i++) {
            GenericRecord record = createEmployeeRecord(topic);
            String name = record.get("name").toString();
            record.put("name", String.valueOf(i) + "::" + record.get("name"));
            simpleKafkaProducer.send(topic, record);

            Thread.sleep(5);
        }
    }

    public void testSendTextMessage(String topic, int rowNumber) throws Exception {
        testSendTextMessage(topic, rowNumber, 5);
    }

    public void testSendTextMessage(String topic, int rowNumber, long sleepTime) throws Exception {
        if (!TopicManager.topicExists(zkServers, brokers)) {
            TopicManager.createTopic(zkServers, topic, 2, 1);
        }

        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer(SimpleKafkaProducer.PRODUCER_TYPE
                .text, getProducerProperties());

        for (int i = 0; i < rowNumber; i++) {
            Object message = String.valueOf(i) + "local-aaa-new-seoeun-new--" + String.valueOf(i);
            simpleKafkaProducer.send(topic, message);

            Thread.sleep(sleepTime);
        }
        log.debug("end send text message");
    }

}