package com.nexr.lean.kafka.util;

import com.nexr.schemaregistry.SchemaClientException;
import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.schemaregistry.SchemaRegistryClient;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalSchemaRegistryClient implements SchemaRegistryClient {

    private final String baseUrl;
    private Map<Integer, SchemaInfo> schemaStore;

    public LocalSchemaRegistryClient(String baseUrl) {
        this.baseUrl = baseUrl;
        this.schemaStore = new HashMap<>();
    }

    @Override
    public String register(String topic, String schema) throws IOException, SchemaClientException {
        SchemaInfo schemaInfo = null;
        List<SchemaInfo> list = getSchemaAllByTopic(topic);
        for (SchemaInfo schemaInfo1 : list) {
            if (schemaInfo1.eqaulsSchema(new Schema.Parser().parse(schema))) {
                schemaInfo = schemaInfo1;
            }
        }
        if (schemaInfo == null) {
            schemaInfo = putSchema(topic, schema);
        }

        return String.valueOf(schemaInfo.getId());
    }

    private SchemaInfo putSchema(String topic, String schema) {
        int id = schemaStore.size();
        SchemaInfo schemaInfo = new SchemaInfo(topic, id, schema, System.currentTimeMillis());
        schemaStore.put(new Integer(id), schemaInfo);
        return schemaInfo;
    }

    @Override
    public SchemaInfo getSchemaByTopicAndId(String topic, String id) throws IOException, SchemaClientException {
        SchemaInfo schemaInfo = schemaStore.get(new Integer(id));
        if (schemaInfo == null) {
            throw new SchemaClientException("Schema Not Found: topic=" + topic + ", id=" + id);
        }
        return schemaInfo;
    }

    @Override
    public SchemaInfo getLatestSchemaByTopic(String topic) throws IOException, SchemaClientException {
        List<Integer> ids = getIdsByTopic(topic);
        if (ids.size() == 0) {
            throw new SchemaClientException("Schema Not Found: topic=" + topic);
        }
        return schemaStore.get(ids.get(0));
    }

    @Override
    public List<SchemaInfo> getSchemaAllByTopic(String topic) throws IOException, SchemaClientException {
        List<SchemaInfo> list = new ArrayList<>();
        List<Integer> ids = getIdsByTopic(topic);
        for (Integer id : ids) {
            list.add(schemaStore.get(id));
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
