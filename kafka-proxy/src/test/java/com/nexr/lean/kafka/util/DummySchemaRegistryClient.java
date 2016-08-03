package com.nexr.lean.kafka.util;

import com.nexr.lean.kafka.serde.CachedSchemaRegistryTest;
import com.nexr.schemaregistry.SchemaClientException;
import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.schemaregistry.SchemaRegistryClient;

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
        SchemaInfo employeeSchema = new SchemaInfo("employee", 1, CachedSchemaRegistryTest.employee_schema_test);
        schemaStore.put(new Integer(1), employeeSchema);
    }

    @Override
    public String register(String topic, String schema) throws IOException, SchemaClientException {
        if (topic.equals("employee")) {
            return String.valueOf(1);
        } else {
            throw new SchemaClientException("Following topic could be registered : employee");
        }
    }

    @Override
    public SchemaInfo getSchemaByTopicAndId(String topic, String id) throws IOException, SchemaClientException {
        if (topic.equals("employee")) {
            return schemaStore.get(new Integer(id));
        } else {
            throw new SchemaClientException("Following topic could be supported : employee");
        }
    }

    @Override
    public SchemaInfo getLatestSchemaByTopic(String topic) throws IOException, SchemaClientException {
        if (topic.equals("employee")) {
            return schemaStore.get(new Integer(1));
        } else {
            throw new SchemaClientException("Following topic could be supported : employee");
        }
    }

    @Override
    public List<SchemaInfo> getSchemaAllByTopic(String topic) throws IOException, SchemaClientException {
        List<SchemaInfo> list = new ArrayList<>();
        if (topic.equals("employee")) {
            list.add(schemaStore.get(new Integer(1)));
        }
        return list;
    }

    @Override
    public List<SchemaInfo> getLatestSchemaAll() throws IOException, SchemaClientException {
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
