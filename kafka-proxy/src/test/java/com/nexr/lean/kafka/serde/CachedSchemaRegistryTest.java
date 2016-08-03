package com.nexr.lean.kafka.serde;

import com.nexr.lean.kafka.util.LocalSchemaRegistryClient;
import com.nexr.schemaregistry.SchemaClientException;
import com.nexr.schemaregistry.SchemaInfo;
import org.junit.Assert;
import org.junit.Test;

public class CachedSchemaRegistryTest {
    public static final String employee_schema_test = "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"employee\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"wrk_dt\", \"type\": \"long\"},\n" +
            "     {\"name\": \"src_info\", \"type\": \"string\"},\n" +
            "     {\n" +
            "         \"name\": \"header\",\n" +
            "         \"type\": {\n" +
            "             \"type\" : \"record\",\n" +
            "             \"name\" : \"headertype\",\n" +
            "             \"fields\" : [\n" +
            "                 {\"name\": \"time\", \"type\": \"long\"}\n" +
            "             ]\n" +
            "         }\n" +
            "      }\n" +
            " ]\n" +
            "}";

    public static final String employee_schema1 = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
            " ]\n" +
            "}";
    //same schema - remove line delimiter from employee_schema1
    public static final String employee_schema2 = "{\"namespace\": \"example.avro\", \"type\": \"record\",\"name\": \"User\", \"fields\": " +
            "[{\"name\": \"name\", \"type\": \"string\"},{\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]} ]}";

    public static final String employee_schema3 = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";
    public static final String ftth_if_schema = "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"ftth_if\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"ontmac\", \"type\": [\"string\", \"null\"]}\n" +
            " ]\n" +
            "}";

    @Test
    public void testCacheRegister() throws Exception {
        LocalSchemaRegistryClient inner = new LocalSchemaRegistryClient("http://hello:18181/repo");

        String id = inner.register("employee", employee_schema1);
        Assert.assertEquals("0", id);
        String id2 = inner.register("ffth_if", ftth_if_schema);
        Assert.assertEquals("1", id2);
        // same schema
        String id3 = inner.register("employee", employee_schema2);
        Assert.assertEquals("0", id3);
        Assert.assertEquals(0, inner.getLatestSchemaByTopic("employee").getId());
        Assert.assertEquals(1, inner.getSchemaAllByTopic("employee").size());

        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(inner);

        try {
            SchemaInfo employeeInfo = cachedSchemaRegistryClient.getSchemaByTopicAndId("employee", String.valueOf(2));
        } catch (SchemaClientException e) {
            Assert.assertTrue(e.getMessage().contains("Schema Not Found"));
        }
        String id4 = cachedSchemaRegistryClient.register("employee", employee_schema3);
        Assert.assertEquals("2", id4);

        // cached
        SchemaInfo employeeInfo2 = cachedSchemaRegistryClient.getSchemaByTopicAndId("employee", String.valueOf(2));
        Assert.assertEquals(2, employeeInfo2.getId());


    }

    @Test
    public void testCacheRegister2() {
        LocalSchemaRegistryClient inner = new LocalSchemaRegistryClient("http://hello:18181/repo");

        try {
            CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(inner);
            String id1 = cachedSchemaRegistryClient.register("employee", employee_schema1);
            Assert.assertEquals("0", id1);
            // cached
            String id2 = cachedSchemaRegistryClient.register("employee", employee_schema1);
            Assert.assertEquals(id1, id2);
            // schema equals
            String id3 = cachedSchemaRegistryClient.register("employee", employee_schema2);
            Assert.assertEquals(id1, id3);

            String id4 = cachedSchemaRegistryClient.register("employee", employee_schema3);
            Assert.assertEquals("1", id4);

            Assert.assertEquals(1, cachedSchemaRegistryClient.getLatestSchemaByTopic("employee").getId());

            // already registered
            Assert.assertEquals("0", cachedSchemaRegistryClient.register("employee", employee_schema1));

        } catch (Exception e) {

        }

    }

    @Test
    public void testCacheGetByTopicAndId() {
        LocalSchemaRegistryClient inner = new LocalSchemaRegistryClient("http://hello:18181/repo");

        try {
            String id = inner.register("employee", employee_schema1);
            Assert.assertEquals("0", id);
            String id2 = inner.register("employee", employee_schema3);
            Assert.assertEquals("1", id2);

            CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(inner);
            SchemaInfo schemaInfo = cachedSchemaRegistryClient.getSchemaByTopicAndId("employee", "0");
            SchemaInfo schemaInfo1 = cachedSchemaRegistryClient.getSchemaByTopicAndId("employee", "0");
            Assert.assertEquals(schemaInfo, schemaInfo1);


        } catch (Exception e) {

        }

    }
}
