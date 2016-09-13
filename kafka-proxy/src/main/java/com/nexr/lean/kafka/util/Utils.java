package com.nexr.lean.kafka.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class Utils {

    private static final char[] symbols;
    private static final Random random = new Random();

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

    public static Map keyValueToMap(Object... keyValues) {
        if ((keyValues.length % 2) != 0) {
            throw new IllegalArgumentException("Key should be pair with value");
        }
        Map map = new HashMap();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i].toString(), keyValues[i + 1].toString());
        }
        return map;
    }

    public static String randomString(int length) {
        if (length < 1) {
            throw new IllegalArgumentException("Length should greater than 0. length=" + length);
        }
        char[] buf = new char[length];
        for (int i = 0; i < length; i++) {
            buf[i] = symbols[random.nextInt(symbols.length)];
        }
        return new String(buf);
    }

    static {
        StringBuilder builder = new StringBuilder();
        for (char ch = '0'; ch <= '9'; ch++) {
            builder.append(ch);
        }
        for (char ch = 'a'; ch <= 'z'; ch++) {
            builder.append(ch);
        }
        symbols = builder.toString().toCharArray();
    }
}
