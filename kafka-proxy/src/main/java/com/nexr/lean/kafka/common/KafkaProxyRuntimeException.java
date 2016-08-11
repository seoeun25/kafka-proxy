package com.nexr.lean.kafka.common;

public class KafkaProxyRuntimeException extends RuntimeException{
    public KafkaProxyRuntimeException() {
        super();
    }

    public KafkaProxyRuntimeException(String message) {
        super(message);
    }

    public KafkaProxyRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaProxyRuntimeException(Throwable cause) {
        super(cause);
    }
}
