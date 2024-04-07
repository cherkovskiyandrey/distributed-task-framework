package com.distributed_task_framework.test_service.exceptions;

import com.distributed_task_framework.test_service.annotations.SagaMethod;

public class SagaMethodNotFoundException extends SagaException {

    public SagaMethodNotFoundException(String method) {
        super("Not exists [%s] for method [%s]".formatted(SagaMethod.class.getSimpleName(), method));
    }
}
