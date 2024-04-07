package com.distributed_task_framework.test_service.exceptions;

import com.distributed_task_framework.test_service.annotations.SagaMethod;

public class SagaMethodResolvingException extends SagaException {

    public SagaMethodResolvingException(String operation) {
        super("Operation [%s] doesn't reference to [%s] method".formatted(operation, SagaMethod.class.getSimpleName()));
    }
}
