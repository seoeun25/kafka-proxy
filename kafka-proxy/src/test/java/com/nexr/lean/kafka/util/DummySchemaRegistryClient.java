package com.nexr.lean.kafka.util;

import com.nexr.lean.kafka.serde.CachedSchemaRegistryTest;
import com.nexr.schemaregistry.SchemaClientException;
import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.schemaregistry.SchemaRegistryClient;
import com.nexr.schemaregistry.Schemas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DummySchemaRegistryClient for testing.
 * It contains pre-defined schemas for testing:
 * <code>employee</code>
 */
public class DummySchemaRegistryClient implements SchemaRegistryClient {

    private final String baseUrl;
    private Map<Integer, SchemaInfo> schemaStore;

    public DummySchemaRegistryClient(String baseUrl) {
        this.baseUrl = baseUrl;
        this.schemaStore = new HashMap<>();
        SchemaInfo employeeSchema = new SchemaInfo("employee", 1, Schemas.employee_schema_test);
        schemaStore.put(new Integer(1), employeeSchema);
        SchemaInfo schemaInfo = new SchemaInfo("az-avro-id", 2, Schemas.employee_schema_test);
        schemaStore.put(new Integer(2), schemaInfo);
        schemaInfo = new SchemaInfo("az-avro-magicbyte-id", 3, Schemas.employee_schema_test);
        schemaStore.put(new Integer(3), schemaInfo);
        schemaInfo = new SchemaInfo("employee-csv-avro", 4, Schemas.employee_schema3);
        schemaStore.put(new Integer(4), schemaInfo);
    }


    @Override
    public String register(String topic, String schema) throws IOException, SchemaClientException {
        if (topic.equals("employee")) {
            return String.valueOf(1);
        } else if (topic.equals("az-avro-id")) {
            return String.valueOf(2);
        } else if (topic.equals("az-avro-magicbyte-id")) {
            return String.valueOf(3);
        } else if (topic.equals("employee-csv-avro")) {
            return String.valueOf(4);
        } else {
            throw new SchemaClientException("Not registered for testing. topic=" + topic);
        }
    }

    @Override
    public SchemaInfo getSchemaByTopicAndId(String topic, String id) throws IOException, SchemaClientException {
        if (!topic.equals("employee") || !topic.equals("az-avro-id") || !topic.equals("az-avro-magicbyte-id") ||
                !topic.equals("employee-csv-avro")) {
            return schemaStore.get(new Integer(id));
        } else {
            throw new SchemaClientException("Not registered for testing. topic=" + topic + ", id=" + id);
        }
    }

    @Override
    public SchemaInfo getLatestSchemaByTopic(String topic) throws IOException, SchemaClientException {
        if (topic.equals("employee")) {
            return schemaStore.get(new Integer(1));
        } else if (topic.equals("az-avro-id")) {
            return schemaStore.get(new Integer(2));
        } else if (topic.equals("az-avro-magicbyte-id")) {
            return schemaStore.get(new Integer(3));
        } else if (topic.equals("employee-csv-avro")) {
            return schemaStore.get(new Integer(4));
        } else {
            throw new SchemaClientException("Not registered for testing. topic=" + topic);
        }
    }

    @Override
    public List<SchemaInfo> getSchemaAllByTopic(String topic) throws IOException, SchemaClientException {
        List<SchemaInfo> list = new ArrayList<>();
        if (topic.equals("employee")) {
            list.add(schemaStore.get(new Integer(1)));
        } else if (topic.equals("az-avro-id")) {
            list.add(schemaStore.get(new Integer(2)));
        } else if (topic.equals("az-avro-magicbyte-id")) {
            list.add(schemaStore.get(new Integer(3)));
        } else if (topic.equals("employee-csv-avro")) {
            list.add(schemaStore.get(new Integer(4)));
        }
        return list;
    }

    @Override
    public List<SchemaInfo> getLatestSchemaAll() throws IOException, SchemaClientException {
        return null;
    }

    @Override
    public List<SchemaInfo> getLatestSchemaByTopics(List<String> topicList) throws IOException, SchemaClientException {
        return null;
    }

    @Override
    public String getResourceByTopic(String topic) throws IOException, SchemaClientException {
        return null;
    }

    /**
     * Gets the reverse sorted id list
     *
     * @param topic
     * @return
     */
    private List<Integer> getIdsByTopic(String topic) {
        Map<Integer, SchemaInfo> schemaInfoMap = new HashMap<>();
        for (Map.Entry<Integer, SchemaInfo> entry : schemaStore.entrySet()) {
            if (entry.getValue().getName().equals(topic)) {
                schemaInfoMap.put(entry.getKey(), entry.getValue());
            }
        }

        List<Integer> sortedIds = new ArrayList<>(schemaInfoMap.keySet());
        Collections.reverse(sortedIds);
        return sortedIds;
    }
}
