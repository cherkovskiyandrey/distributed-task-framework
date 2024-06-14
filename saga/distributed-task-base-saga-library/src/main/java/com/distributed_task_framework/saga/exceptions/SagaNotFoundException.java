package com.distributed_task_framework.saga.exceptions;

public class SagaNotFoundException extends SagaException {
    public SagaNotFoundException(String message) {
        super(message);
    }
}
