package com.nexr.lean.kafka.common;

import java.util.Properties;

public class ConfigUtils {

    public static Properties keyValueToProperties(Object... keyValues) {
        if ((keyValues.length % 2) != 0) {
            throw new IllegalArgumentException("Key should be pair with value");
        }
        Properties properties = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            properties.setProperty(keyValues[i].toString(), keyValues[i + 1].toString());
        }
        return properties;
    }
}
