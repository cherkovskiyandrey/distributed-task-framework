package com.distributed_task_framework.test_service.exceptions;

public class SagaMethodNotFoundException extends SagaInternalException {

    public SagaMethodNotFoundException(String message) {
        super(message);
    }
}
