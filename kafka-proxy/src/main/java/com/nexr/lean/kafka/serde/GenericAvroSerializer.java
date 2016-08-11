package com.nexr.lean.kafka.serde;

import com.nexr.schemaregistry.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class GenericAvroSerializer implements Serializer<GenericRecord> {

    private static Logger log = LoggerFactory.getLogger(GenericAvroSerializer.class);
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private GenericAvroSerde.Meta meta = null;
    private CachedSchemaRegistryClient schemarRegistry = null;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        meta = GenericAvroSerde.getHeader(configs.get(AvroSerdeConfig.HEADER_META_NAME_CONFIG));
        Object url = configs.get(AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        try {
            log.debug("schemaRegistry baseUrl {} ", url);
            SchemaRegistryClient actualClient = GenericAvroSerde.initializeSchemaRegistry(configs.get(AvroSerdeConfig
                    .SCHEMA_REGISTRY_CLASS_CONFIG).toString(), url.toString());
            log.debug("actualSchemaRegistry {} ", actualClient.getClass().getName());
            schemarRegistry = new CachedSchemaRegistryClient(actualClient);
        } catch (Exception e) {
            throw new ConfigException("Fail to initialize SchemaRegitry", e);
        }
    }

    @Override
    public byte[] serialize(String topic, GenericRecord data) {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            Schema schema = data.getSchema();
            String id = schemarRegistry.register(topic, schema.toString());
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

            meta.write(out, Integer.parseInt(id), topic);
            Encoder e = encoderFactory.binaryEncoder(out, null);
            writer.write(data, e);
            e.flush();
            byte[] byteData = out.toByteArray();
            return byteData;
        } catch (Exception e) {
            throw new SerializationException("Fail to serialize Avro message", e);
        } finally {
            try {
                out.close();
            } catch (IOException e) {

            }
        }
    }

    @Override
    public void close() {

    }
}
