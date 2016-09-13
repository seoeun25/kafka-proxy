package com.nexr.lean.kafka.serde;

import com.nexr.lean.kafka.util.LocalSchemaRegistryClient;
import com.nexr.schemaregistry.SchemaClientException;
import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.schemaregistry.Schemas;
import org.junit.Assert;
import org.junit.Test;

public class CachedSchemaRegistryTest {

    @Test
    public void testCacheRegister() throws Exception {
        LocalSchemaRegistryClient inner = new LocalSchemaRegistryClient("http://hello:18181/repo");

        String id = inner.register("employee", Schemas.employee_schema1);
        Assert.assertEquals("0", id);
        String id2 = inner.register("ffth_if", Schemas.ftth_if_schema);
        Assert.assertEquals("1", id2);
        // same schema
        String id3 = inner.register("employee", Schemas.employee_schema2);
        Assert.assertEquals("0", id3);
        Assert.assertEquals(0, inner.getLatestSchemaByTopic("employee").getId());
        Assert.assertEquals(1, inner.getSchemaAllByTopic("employee").size());

        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(inner);

        try {
            SchemaInfo employeeInfo = cachedSchemaRegistryClient.getSchemaByTopicAndId("employee", String.valueOf(2));
        } catch (SchemaClientException e) {
            Assert.assertTrue(e.getMessage().contains("Schema Not Found"));
        }
        String id4 = cachedSchemaRegistryClient.register("employee", Schemas.employee_schema3);
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
            String id1 = cachedSchemaRegistryClient.register("employee", Schemas.employee_schema1);
            Assert.assertEquals("0", id1);
            // cached
            String id2 = cachedSchemaRegistryClient.register("employee", Schemas.employee_schema1);
            Assert.assertEquals(id1, id2);
            // schema equals
            String id3 = cachedSchemaRegistryClient.register("employee", Schemas.employee_schema2);
            Assert.assertEquals(id1, id3);

            String id4 = cachedSchemaRegistryClient.register("employee", Schemas.employee_schema3);
            Assert.assertEquals("1", id4);

            Assert.assertEquals(1, cachedSchemaRegistryClient.getLatestSchemaByTopic("employee").getId());

            // already registered
            Assert.assertEquals("0", cachedSchemaRegistryClient.register("employee", Schemas.employee_schema1));

        } catch (Exception e) {

        }

    }

    @Test
    public void testCacheGetByTopicAndId() {
        LocalSchemaRegistryClient inner = new LocalSchemaRegistryClient("http://hello:18181/repo");

        try {
            String id = inner.register("employee", Schemas.employee_schema1);
            Assert.assertEquals("0", id);
            String id2 = inner.register("employee", Schemas.employee_schema3);
            Assert.assertEquals("1", id2);

            CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(inner);
            SchemaInfo schemaInfo = cachedSchemaRegistryClient.getSchemaByTopicAndId("employee", "0");
            SchemaInfo schemaInfo1 = cachedSchemaRegistryClient.getSchemaByTopicAndId("employee", "0");
            Assert.assertEquals(schemaInfo, schemaInfo1);


        } catch (Exception e) {

        }

    }
}
