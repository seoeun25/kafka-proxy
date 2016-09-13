package com.nexr.lean.kafka.serde;

import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.schemaregistry.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVRecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class CSVToAvroSerializer implements Serializer<byte[]> {

    private static Logger log = LoggerFactory.getLogger(CSVToAvroSerializer.class);
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private GenericAvroSerde.Meta meta = null;
    private CachedSchemaRegistryClient schemarRegistry = null;
    private String csvDelimiter = ",";
    private CSVProperties csvProperties = null;
    private CSVRecordParser<GenericData.Record> csvParser = null;

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
            if (!configs.containsKey(AvroSerdeConfig.CSV_DELIMITER_CONFIG)) {
                log.info("No configs for {}, Use default=[{}]", AvroSerdeConfig.CSV_DELIMITER_CONFIG, csvDelimiter);
            } else {
                csvDelimiter = configs.get(AvroSerdeConfig.CSV_DELIMITER_CONFIG).toString();
            }
            csvProperties = new CSVProperties.Builder().delimiter(csvDelimiter).build();
        } catch (Exception e) {
            throw new ConfigException("Fail to configure.", e);
        }
    }

    @Override
    public byte[] serialize(String topic, byte[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            SchemaInfo schemaInfo = schemarRegistry.getSchemaByTopic(topic);
            Schema schema = schemaInfo.parseSchema();
            String id = String.valueOf(schemaInfo.getId());
            GenericData.Record record = convertToRecord(schema, new String(data));
            GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(schema);

            meta.write(out, Integer.parseInt(id), topic);
            Encoder e = encoderFactory.binaryEncoder(out, null);
            writer.write(record, e);
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

    public GenericData.Record convertToRecord(Schema schema, String line) {
        log.trace("line : [{}]", line);
        if (csvParser == null) {
            csvParser = new CSVRecordParser<>(csvProperties, schema, GenericData.Record.class, null);
        }
        return csvParser.read(line);
    }

    @Override
    public void close() {

    }
}
