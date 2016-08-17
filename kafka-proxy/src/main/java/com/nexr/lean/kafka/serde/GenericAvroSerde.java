package com.nexr.lean.kafka.serde;

import com.nexr.schemaregistry.SchemaRegistryClient;
import com.nexr.schemaregistry.SimpleSchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Map;

public class GenericAvroSerde implements Serde<GenericRecord> {

    public static final byte MAGIC_BYTE = 0x0;

    private final Serde<GenericRecord> serde;

    public GenericAvroSerde() {
        serde = Serdes.serdeFrom(new GenericAvroSerializer(), new GenericAvroDeserializer());
    }

    public static Meta getHeader(Object name) {
        try {
            return Meta.valueOf(name.toString());
        } catch (NullPointerException | IllegalArgumentException e) {
            return Meta.ID;
        }
    }

    public static SchemaRegistryClient initializeSchemaRegistry(String className, String url)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        SchemaRegistryClient actualClient = null;
        if (className != null) {
            Constructor constructor = Class.forName(className).getConstructor(String.class);
            actualClient = (SchemaRegistryClient) constructor.newInstance(url.toString());
        } else {
            actualClient = new SimpleSchemaRegistryClient(url);
        }
        return actualClient;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serde.serializer().configure(configs, isKey);
        serde.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        serde.serializer().close();
        serde.deserializer().close();
    }

    @Override
    public Serializer<GenericRecord> serializer() {
        return serializer();
    }

    @Override
    public Deserializer<GenericRecord> deserializer() {
        return deserializer();
    }

    public static enum Meta {

        MAGICBYTE_ID { //used in camus

            @Override
            public void write(ByteArrayOutputStream out, int id, String topic) throws IOException {
                out.write(MAGIC_BYTE);
                out.write(ByteBuffer.allocate(4).putInt(id).array());
            }

            @Override
            public String describe() {
                return "MagicByte and SchemaId(4byte)";
            }

            @Override
            public int size() {
                return 5;
            }

            @Override
            public boolean containMagicByte() {
                return true;
            }
        },
        ID {
            @Override
            public void write(ByteArrayOutputStream out, int id, String topic) throws IOException {
                out.write(ByteBuffer.allocate(4).putInt(id).array());
            }

            public String describe() {
                return "SchemaId(int) from SchemaRegistry in 4 byte";
            }

            @Override
            public int size() {
                return 4;
            }
        },
        ID_SUBJECT {
            @Override
            public void write(ByteArrayOutputStream out, int id, String topic) throws IOException {

            }

            @Override
            public String describe() {
                return "SchemaId(4 byte) + subject()";
            }

            @Override
            public int size() {
                return 20;
            }
        };

        abstract public void write(ByteArrayOutputStream out, int id, String topic) throws IOException;

        abstract public String describe();

        abstract public int size();

        public boolean containMagicByte() {
            return false;
        }

    }
}
