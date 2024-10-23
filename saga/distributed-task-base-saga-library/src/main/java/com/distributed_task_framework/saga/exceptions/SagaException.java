package com.distributed_task_framework.saga.exceptions;

public class SagaException extends RuntimeException {

    public SagaException(String message) {
        super(message);
    }

    public SagaException(String message, Throwable cause) {
        super(message, cause);
    }

    public SagaException(Exception e) {
        super(e);
    }
}
