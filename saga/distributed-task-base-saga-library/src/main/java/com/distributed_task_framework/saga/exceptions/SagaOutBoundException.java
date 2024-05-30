package com.distributed_task_framework.saga.exceptions;

public class SagaOutBoundException extends SagaInternalException {
    public SagaOutBoundException(String message) {
        super(message);
    }
}
