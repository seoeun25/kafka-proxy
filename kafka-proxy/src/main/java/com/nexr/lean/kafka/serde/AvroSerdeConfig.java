package com.nexr.lean.kafka.serde;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class AvroSerdeConfig extends AbstractConfig {

    public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    public static final String SCHEMA_REGISTRY_CLASS_CONFIG = "schema.registry.class";
    public static final String SCHEMA_REGISTRY_CLASS_DOC = "Deserializer class for schema registry client that implements the " +
            "<code>SchemaRegistryClient</code> interface.";
    public static final String HEADER_META_NAME_CONFIG = "avro.meta.name";
    public static final String HEADER_META_NAME_DOC = "The name of meta format for avro message. This defined in " +
            "<code>GenericAvroSerde.Meta<code> enum.";
    private static final String SCHEMA_REGISTRY_URL_DOC = "The url of schema registry service";


    public AvroSerdeConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }
}
