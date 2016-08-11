package com.nexr.lean.kafka.util;

import com.nexr.lean.kafka.common.KafkaProxyException;
import com.nexr.lean.kafka.serde.AvroSerdeConfig;
import com.nexr.lean.kafka.serde.GenericAvroSerializer;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SimpleKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(SimpleKafkaProducer.class);

    private MessageProducer messageProducer;

    private PRODUCER_TYPE type = PRODUCER_TYPE.avro;
    private Properties context;

    public SimpleKafkaProducer(PRODUCER_TYPE producerType, Properties properties) {
        this.type = producerType;
        this.context = properties;
        try {
            messageProducer = createMessageProducer(context, type);
            log.info("MessageProducer : {}", messageProducer.getClass().getName());
        } catch (KafkaProxyException e) {
            log.warn("SimpleKafkaProducer initializing failed", e);
        }
    }

    public MessageProducer createMessageProducer(Properties context, PRODUCER_TYPE type) throws KafkaProxyException {
        MessageProducer messageProducer = null;
        if (type.equals(PRODUCER_TYPE.text)) {
            messageProducer = new TextProducer(context);
        } else {
            messageProducer = new DefaultProducer(context);
        }
        return messageProducer;
    }

    /**
     * Sends the data to kafka topic
     *
     * @param topic a topic name
     * @param data  a data to send.
     *              Type should be match with Serializer.
     *              <ul>
     *              <li>{@linkplain com.nexr.lean.kafka.util.SimpleKafkaProducer.PRODUCER_TYPE#text} <tt>String</tt> </li>
     *              <li>{@linkplain com.nexr.lean.kafka.util.SimpleKafkaProducer.PRODUCER_TYPE#avro} <tt>GenericRecord</tt> </li>
     *              </ul>
     * @throws KafkaProxyException
     */
    public void send(String topic, Object data) throws KafkaProxyException {
        messageProducer.send(topic, data);
    }

    public void close() {
        messageProducer.close();
    }

    public static enum PRODUCER_TYPE {
        /**
         * String Message Format
         */
        text,
        /**
         * Avro Message Format
         */
        avro
    }

    public static abstract class MessageProducer<K, V> {

        protected KafkaProducer producer;
        protected Properties properties;
        protected Sender sender;
        protected SenderCallback failedCallback = new SenderCallback();

        public MessageProducer(Properties context) throws KafkaProxyException {
            try {
                properties = context;
                properties.putAll(getSerde());
                producer = createKafkaProduer(properties);
                sender = new Sender();
            } catch (Exception e) {
                throw new KafkaProxyException("Fail to create Producer : " + e.getMessage(), e);
            }
        }

        public KafkaProducer createKafkaProduer(Properties properties) {
            return new KafkaProducer<>(properties);
        }

        public void send(String topic, V object) throws KafkaProxyException {
            ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, object);
            sender.send(producer, producerRecord, failedCallback);
        }

        public void close() {
            producer.close();
        }

        public Map<String, Object> getSerde() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class.getName());
            return props;
        }
    }

    public static class DefaultProducer extends MessageProducer<String, Object> {

        public DefaultProducer(Properties context) throws KafkaProxyException {
            super(context);
            if (!properties.contains(AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG)) {
                log.warn("SCHEMA_REGISTRY_CLASS_CONFIG is {}", properties.get(AvroSerdeConfig.SCHEMA_REGISTRY_CLASS_CONFIG));
            }
            if (!properties.contains(AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG)) {
                log.warn("SCHEMA_REGISTRY_CLASS_CONFIG is {}", properties.get(AvroSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG));
            }
            if (!properties.contains(AvroSerdeConfig.HEADER_META_NAME_CONFIG)) {
                log.warn("HEADER_META_NAME_CONFIG is {}", properties.get(AvroSerdeConfig.HEADER_META_NAME_CONFIG));
            }
        }
    }

    public static class TextProducer extends MessageProducer<String, String> {

        public TextProducer(Properties context) throws KafkaProxyException {
            super(context);
        }

        public Map<String, Object> getSerde() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            return props;
        }
    }

    static class Sender {
        public boolean send(KafkaProducer kafkaProducer, final ProducerRecord record, final SenderCallback callback) throws
                KafkaProxyException {
            try {
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            try {
                                callback.onFail(record, exception);
                            } catch (KafkaProxyException e) {
                                e.printStackTrace();
                            }
                        } else {
                            callback.onSucceed(metadata);
                        }
                    }
                });
                return true;
            } catch (BufferExhaustedException e) {
                callback.onFail(record, e);
                return false;
            }
        }
    }

    static class SenderCallback {

        public void onFail(ProducerRecord record, Throwable throwable) throws KafkaProxyException {
            throw new KafkaProxyException("SentFailed record : " + record.toString(), throwable);
        }

        public void onSucceed(RecordMetadata metadata) {
            log.trace("sent succeed : tooic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset
                    ());

        }
    }
}
