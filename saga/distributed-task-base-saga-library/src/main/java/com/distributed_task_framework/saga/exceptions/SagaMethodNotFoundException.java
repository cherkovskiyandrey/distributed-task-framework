package com.distributed_task_framework.saga.exceptions;

public class SagaMethodNotFoundException extends SagaInternalException {

    public SagaMethodNotFoundException(String message) {
        super(message);
    }
}
