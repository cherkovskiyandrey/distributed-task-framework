package com.distributed_task_framework.saga.exceptions;

public class SagaMethodResolvingException extends SagaInternalException {

    public SagaMethodResolvingException(String message) {
        super(message);
    }

    public SagaMethodResolvingException(Exception e) {
        super(e);
    }
}
