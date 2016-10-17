package com.nexr.lean.kafka.serde;

import com.nexr.lean.kafka.OffsetManager;
import com.nexr.lean.kafka.consumer.ConsumerService;
import com.nexr.lean.kafka.consumer.SimpleConsumerConfig;
import com.nexr.lean.kafka.util.SimpleKafakProducerExample;
import com.nexr.lean.kafka.util.TestServers;
import com.nexr.lean.kafka.util.Utils;
import com.nexr.schemaregistry.SchemaClientException;
import com.nexr.schemaregistry.SchemaRegistryClient;
import com.nexr.schemaregistry.Schemas;
import com.nexr.schemaregistry.SimpleSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.DatasetRecordException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CSVToAvroSerializerTest {

    private static Logger log = LoggerFactory.getLogger(CSVToAvroSerializerTest.class);

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
    public void testCreateRecord() {
        CSVToAvroSerializer serializer1 = new CSVToAvroSerializer();
        serializer1.configure(Utils.keyValueToMap(
                AvroSerdeConfig.CSV_DELIMITER_CONFIG, ",",
                AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass,
                AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        ), false);

        // schema include int type
        Schema schema1 = new Schema.Parser().parse(Schemas.employee_schema5);

        String line1 = "name-az,5,purple";
        GenericData.Record record = serializer1.convertToRecord(schema1, line1);
        log.info("--- record = {}", record);
        Assert.assertNotNull(record);
        Assert.assertEquals(record.get("name").toString(), "name-az");
        Assert.assertEquals(record.get("favorite_number"), new Integer(5));
        Assert.assertEquals(record.get("favorite_color"), "purple");

        String line2 = "hello-az, 9, black";
        try {
            // can not parse as Integer if there is space
            serializer1.convertToRecord(schema1, line2);
            Assert.fail("Should not parse as integer type if there is space");
        } catch (DatasetRecordException e) {
        }

        CSVToAvroSerializer serializer2 = new CSVToAvroSerializer();

        serializer2.configure(Utils.keyValueToMap(
                AvroSerdeConfig.CSV_DELIMITER_CONFIG, ",",
                AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass,
                AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        ), false);
        // all string type.
        Schema schema2 = new Schema.Parser().parse(Schemas.employee_schema3);
        record = serializer2.convertToRecord(schema2, line2);
        Assert.assertNotNull(record);
        Assert.assertEquals(record.get("name").getClass(), String.class);
        Assert.assertEquals(record.get("favorite_number"), " 9");
        Assert.assertEquals(record.get("favorite_color"), " black");

    }

    @Test
    public void testCSVToAvroAndDeserialize() throws InterruptedException{
        String topic = "employee-csv-avro";

        try {
            SchemaRegistryClient actualClient = GenericAvroSerde.initializeSchemaRegistry(schemaRegistryClass, schemaRegistryUrl);
            log.info("---- actualClient = {}", actualClient.getClass().getName());

            log.info("schema for {} : \n{}", topic, Schemas.employee_schema3);
            String id = actualClient.register(topic, Schemas.employee_schema3);
            log.info("schema id = {}", id);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        try {
            SimpleKafakProducerExample simpleKafakProducerExample = new SimpleKafakProducerExample(zkServers, brokers,
                    schemaRegistryClass, schemaRegistryUrl);
            simpleKafakProducerExample.testSendCSVToAvroType(topic, 10, 5);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // consume the avro message from kafka
        Properties consumerProperties = Utils.keyValueToProperties(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, "az-group-csv",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class.getName(),
                AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass,
                AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                AvroSerdeConfig.HEADER_META_NAME_CONFIG, "ID",
                SimpleConsumerConfig.ENABLE_MANUAL_COMMIT_CONFIG, "false"
        );

        final List<ConsumerRecord<String, GenericRecord>> list = new ArrayList<>();
        ConsumerService consumerService = ConsumerService.getInstance();
        consumerService.fetchAsync(topic, 3000, 10, consumerProperties, new ConsumerService.FetchCallback<String, GenericRecord>() {
            @Override
            public void onComplete(List<? extends ConsumerRecord<String, GenericRecord>> consumerRecords, Exception e) {
                log.info("fetch: size={}", consumerRecords.size());
                for (ConsumerRecord record : consumerRecords) {
                    log.info("record value class = {}", record.value().getClass().getName());
                }
                list.addAll(consumerRecords);
            }
        });

        Thread.sleep(5000);
        log.info("----- list size={}", list.size());
        Assert.assertTrue(list.size() > 0);
        Assert.assertEquals(GenericData.Record.class, list.get(0).value().getClass());
        GenericData.Record record = (GenericData.Record) list.get(0).value();
        for (ConsumerRecord<String, GenericRecord> consumerRecord: list) {
            GenericData.Record record1 = (GenericData.Record) consumerRecord.value();
            log.info("consumerRecord = [{},{},{}]", record1.get(0), record1.get(1), record1.get(2));
        }
        Assert.assertEquals("9", record.get("favorite_number").toString());
    }

}
