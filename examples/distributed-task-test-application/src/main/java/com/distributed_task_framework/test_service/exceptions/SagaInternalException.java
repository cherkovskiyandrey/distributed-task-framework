package com.distributed_task_framework.test_service.exceptions;

public class SagaInternalException extends RuntimeException {
    public SagaInternalException(String message) {
        super(message);
    }

    public SagaInternalException(Exception e) {
        super(e);
    }
}
