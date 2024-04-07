package com.distributed_task_framework.test_service.exceptions;

public class SagaTaskNotFoundException extends SagaException {

    public SagaTaskNotFoundException(String sagaMethod) {
        super("Not exist dtf task [%s] for saga method.".formatted(sagaMethod));
    }
}
