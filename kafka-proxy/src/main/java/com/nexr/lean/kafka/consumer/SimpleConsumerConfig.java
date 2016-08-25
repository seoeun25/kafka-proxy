package com.nexr.lean.kafka.consumer;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class SimpleConsumerConfig extends AbstractConfig {

    public static final String ENABLE_MANUAL_COMMIT_CONFIG = "enable.manual.commit";
    private static final String ENABLE_MANUAL_COMMIT_DOC = "If true the consumer's offset will be committed on complete fetch task";


    public SimpleConsumerConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }
}
