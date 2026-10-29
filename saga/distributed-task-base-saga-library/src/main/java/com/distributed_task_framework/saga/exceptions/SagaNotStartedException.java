package com.distributed_task_framework.saga.exceptions;

public class SagaNotStartedException extends SagaInternalException {

    public SagaNotStartedException(Exception e) {
        super(e);
    }

    public SagaNotStartedException(String message, Throwable cause) {
        super(message, cause);
    }
}
