package com.nexr.lean.kafka.common;

public class KafkaProxyException extends Exception{
    public KafkaProxyException() {
        super();
    }

    public KafkaProxyException(String message) {
        super(message);
    }

    public KafkaProxyException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaProxyException(Throwable cause) {
        super(cause);
    }
}
