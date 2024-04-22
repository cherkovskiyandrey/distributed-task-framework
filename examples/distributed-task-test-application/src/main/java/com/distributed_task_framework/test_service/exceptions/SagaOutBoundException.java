package com.distributed_task_framework.test_service.exceptions;

public class SagaOutBoundException extends SagaInternalException {
    public SagaOutBoundException(String message) {
        super(message);
    }
}
