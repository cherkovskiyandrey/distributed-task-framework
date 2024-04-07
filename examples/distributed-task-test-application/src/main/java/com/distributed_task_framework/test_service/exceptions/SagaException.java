package com.distributed_task_framework.test_service.exceptions;

public class SagaException extends RuntimeException {
    public SagaException(String message) {
        super(message);
    }
}
