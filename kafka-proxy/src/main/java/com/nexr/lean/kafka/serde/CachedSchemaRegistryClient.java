package com.nexr.lean.kafka.serde;

import com.nexr.schemaregistry.SchemaClientException;
import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.schemaregistry.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CachedSchemaRegistryClient {

    private static Logger log = LoggerFactory.getLogger(CachedSchemaRegistryClient.class);
    private ConcurrentHashMap<Integer, SchemaInfo> schemaStore;
    private List schemaList = Collections.synchronizedList(new ArrayList());
    private SchemaRegistryClient inner;

    public CachedSchemaRegistryClient(SchemaRegistryClient inner) {
        this.inner = inner;
        schemaStore = new ConcurrentHashMap<>();
    }

    public String register(String topic, String schema) throws IOException, SchemaClientException {
        SchemaInfo schemaInfo = null;
        // check cache
        List<Integer> ids = getIdsByTopic(topic);
        if (ids.size() == 0) { // no cached
            String id = inner.register(topic, schema);
            schemaInfo = new SchemaInfo(topic, Integer.parseInt(id), schema);
            schemaStore.put(new Integer(id), schemaInfo);
            log.debug("no cache, inner.register. id {}, topic {}", id, topic);
        } else {
            for (Integer id : ids) {
                if (schemaStore.get(id).eqaulsSchema(new Schema.Parser().parse(schema))) {
                    schemaInfo = schemaStore.get(id);
                    log.debug("get from cache. id {}, topic {}", id, topic);
                }
            }
            if (schemaInfo == null) {
                schemaInfo = new SchemaInfo(topic, Integer.parseInt(inner.register(topic, schema)), schema);
                log.debug("cached but other schemas under topic {}, inner.register=> id {} ", topic, schemaInfo.getId());
            }
        }

        return String.valueOf(schemaInfo.getId());
    }

    public SchemaInfo getSchemaByTopicAndId(String topic, String id) throws IOException, SchemaClientException {
        SchemaInfo schemaInfo = null;
        if (schemaStore.keySet().contains(new Integer(id))) {
            schemaInfo = schemaStore.get(new Integer(id));
            if (!schemaInfo.getName().equals(topic)) {
                log.warn("Cached schemaInfo of id[{}] has wrong topic [{}], expected [{}]", id, schemaInfo.getName(), topic);
                schemaInfo = inner.getSchemaByTopicAndId(topic, id);
                schemaStore.put(Integer.valueOf(id), schemaInfo);
            }
        } else {
            log.info("not cached for {} {}, get from inner scheamRegistry", id, topic);
            schemaInfo = inner.getSchemaByTopicAndId(topic, id);
            schemaStore.put(new Integer(id), schemaInfo);
        }
        return schemaInfo;
    }

    public SchemaInfo getLatestSchemaByTopic(String topic) throws IOException, SchemaClientException {
        return inner.getLatestSchemaByTopic(topic);
    }

    public List<SchemaInfo> getLatestSchemaAll() throws IOException, SchemaClientException {
        return inner.getLatestSchemaAll();
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
