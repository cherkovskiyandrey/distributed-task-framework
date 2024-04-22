package com.distributed_task_framework.test_service.exceptions;

public class SagaMethodResolvingException extends SagaInternalException {

    public SagaMethodResolvingException(Exception e) {
        super(e);
    }
}
