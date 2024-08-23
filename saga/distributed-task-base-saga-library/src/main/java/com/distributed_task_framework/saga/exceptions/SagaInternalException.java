package com.distributed_task_framework.saga.exceptions;

public class SagaInternalException extends SagaException {
    public SagaInternalException(String message) {
        super(message);
    }

    public SagaInternalException(Exception e) {
        super(e);
    }

    public SagaInternalException(String message, Throwable cause) {
        super(message, cause);
    }
}
