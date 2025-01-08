package com.distributed_task_framework.test.autoconfigure.exception;

public class FailedCancellationException extends RuntimeException {

    public FailedCancellationException(Throwable cause) {
        super(cause);
    }
}
