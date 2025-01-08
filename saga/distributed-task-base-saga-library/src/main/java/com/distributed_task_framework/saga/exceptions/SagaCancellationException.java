package com.distributed_task_framework.saga.exceptions;

public class SagaCancellationException extends SagaException {

    public SagaCancellationException(String message) {
        super(message);
    }

    public SagaCancellationException(String message, Throwable cause) {
        super(message, cause);
    }
}
