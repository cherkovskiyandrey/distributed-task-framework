package com.distributed_task_framework.test_service.exceptions;

public class SagaException extends RuntimeException {

    public SagaException(String message) {
        super(message);
    }

    public SagaException(String message, Throwable cause) {
        super(message, cause);
    }

    public SagaException(Throwable cause) {
        super(cause);
    }

    public SagaException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public SagaException(Exception e) {
        super(e);
    }
}
