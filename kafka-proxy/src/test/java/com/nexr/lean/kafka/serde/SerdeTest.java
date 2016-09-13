package com.nexr.lean.kafka.serde;

import com.nexr.schemaregistry.Schemas;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SerdeTest {

    private static StringSerializer stringSerializer;
    private static StringDeserializer stringDeserializer;

    private static String schemaRegistryClass;
    private static String schemaRegistryUrl;

    @BeforeClass
    public static void setupClass() {
        stringSerializer = new StringSerializer();
        stringDeserializer = new StringDeserializer();
    }

    @Test
    public void testStringSerde() {
        String topic = "test-topic";
        String abc = "abc";
        Assert.assertEquals(abc, stringDeserializer.deserialize(topic, stringSerializer.serialize(topic, abc)));
    }

    @Test
    public void testAvroSerde_ID() {
        testAvroSerde("ID");
    }

    @Test
    public void testAvroSerde_MAGICBYTE_ID() {
        testAvroSerde("MAGICBYTE_ID");
    }

    private void testAvroSerde(String serdeMetaName) {
        String topic = "employee";
        Map<String, String> configMap = new HashMap<>();
        configMap.put(AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, schemaRegistryClass);
        configMap.put(AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        configMap.put(AvroSerdeConfig.HEADER_META_NAME_CONFIG, serdeMetaName);
        GenericAvroSerializer avroSerializer = new GenericAvroSerializer();
        avroSerializer.configure(configMap, false);
        GenericAvroDeserializer avroDeserializer = new GenericAvroDeserializer();
        avroDeserializer.configure(configMap, false);

        Schema employeeSchema = new Schema.Parser().parse(Schemas.employee_schema_test);
        Schema headerSchema = employeeSchema.getField("header").schema();

        GenericRecord record = new GenericData.Record(employeeSchema);
        record.put("name", "seoeun");
        record.put("favorite_number", String.valueOf(9));
        record.put("wrk_dt", System.currentTimeMillis());
        record.put("src_info", topic);
        GenericRecord header = new GenericData.Record(headerSchema);
        header.put("time", record.get("wrk_dt"));
        record.put("header", header);

        byte[] serialized = avroSerializer.serialize(topic, record);
        GenericRecord deserialized = avroDeserializer.deserialize(topic, serialized);

        Assert.assertEquals(record.get("name").toString(), deserialized.get("name").toString());
        Assert.assertEquals(record.get("favorite_number").toString(), deserialized.get("favorite_number").toString());
    }

    @Test
    public void testGetAvroSerdeMeta() {

        GenericAvroSerde.Meta meta = GenericAvroSerde.getHeader(null);
        Assert.assertNotNull(meta);
        Assert.assertEquals(GenericAvroSerde.Meta.ID, meta);

        meta = GenericAvroSerde.getHeader("abc");
        Assert.assertNotNull(meta);
        Assert.assertEquals(GenericAvroSerde.Meta.ID, meta);

        meta = GenericAvroSerde.getHeader("MAGICBYTE_ID");
        Assert.assertEquals(GenericAvroSerde.Meta.MAGICBYTE_ID, meta);

    }

    @Test
    public void testIntializeSchemaRegistry() throws Exception {
        String clasz = "com.nexr.lean.kafka.util.LocalSchemaRegistryClient";
        String url = "http://hello:18181/repo";

        GenericAvroSerializer serializer = new GenericAvroSerializer();
        Map<String, String> configMap = new HashMap<>();
        configMap.put(AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG, clasz);
        configMap.put(AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, url);

        serializer.configure(configMap, false);
    }

    @Test
    public void test() {

        Boolean b1 = new Boolean("true");
        System.out.println("b1 : " + b1.booleanValue());

        b1 = new Boolean(null);
        System.out.println("b1 null: " + b1.booleanValue());
        b1 = new Boolean("false");
        System.out.println("b1 false: " + b1.booleanValue());
        b1 = new Boolean("hello");
        System.out.println("b1 hello: " + b1.booleanValue());

    }

    static {
        schemaRegistryClass = "com.nexr.lean.kafka.util.DummySchemaRegistryClient";
        schemaRegistryUrl = "http://hello:18181/repo";
    }


}
