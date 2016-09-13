package com.nexr.lean.kafka.serde;

import com.nexr.lean.kafka.common.KafkaProxyRuntimeException;
import com.nexr.schemaregistry.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

public class GenericAvroDeserializer implements Deserializer<GenericRecord> {

    private static Logger log = LoggerFactory.getLogger(GenericAvroSerializer.class);
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private GenericAvroSerde.Meta meta = null;
    private CachedSchemaRegistryClient schemarRegistry;
    private Schema schemaProperty;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        meta = GenericAvroSerde.getHeader(configs.get(AvroSerdeConfig.HEADER_META_NAME_CONFIG));
        if (meta == GenericAvroSerde.Meta.NONE) {
            try {
                String schemaDefinition = configs.get(AvroSerdeConfig.SCHEMA_DEFINITION_CONFIG).toString();
                schemaProperty = new Schema.Parser().parse(schemaDefinition);
            } catch ( Exception e) {
                throw new ConfigException("Fail to initialize schema for schema_definition : " + configs.get(AvroSerdeConfig
                        .SCHEMA_DEFINITION_CONFIG));
            }
        } else {
            Object url = configs.get(AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG);
            try {
                log.debug("schemaRegistry baseUrl {} ", url);
                SchemaRegistryClient actualClient = GenericAvroSerde.initializeSchemaRegistry(configs.get(AvroSerdeConfig
                        .SCHEMA_REGISTRY_CLASS_CONFIG).toString(), url.toString());
                log.debug("actualSchemaRegistry {} ", actualClient.getClass().getName());
                schemarRegistry = new CachedSchemaRegistryClient(actualClient);
            } catch (Exception e) {
                throw new ConfigException("Fail to initialize SchemaRegistry", e);
            }
        }
    }

    private ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (meta.containMagicByte()) {
            if (buffer.get() != GenericAvroSerde.MAGIC_BYTE) {
                throw new SerializationException("Unknown magic byte!");
            }
        }
        return buffer;
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        int id = -1;
        try {
            ByteBuffer byteBuffer = getByteBuffer(data);
            id = byteBuffer.getInt();
            Schema schema = meta == GenericAvroSerde.Meta.NONE ? schemaProperty :
                    schemarRegistry.getSchemaByTopicAndId(topic, String.valueOf(id)) .parseSchema();
            int length = byteBuffer.limit() - meta.size();
            int start = byteBuffer.position() + byteBuffer.arrayOffset();
            DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(schema);
            GenericData.Record record = datumReader.read(null, decoderFactory.binaryDecoder(byteBuffer.array(), start, length, null));
            return record;

        } catch (Exception e) {
            throw new KafkaProxyRuntimeException("Fail to deserialize avro message for id " + id, e);
        }
    }

    @Override
    public void close() {

    }
}
