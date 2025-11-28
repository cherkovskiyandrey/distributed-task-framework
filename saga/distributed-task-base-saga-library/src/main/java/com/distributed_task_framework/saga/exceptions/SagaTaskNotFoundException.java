package com.distributed_task_framework.saga.exceptions;

public class SagaTaskNotFoundException extends SagaInternalException {

    public SagaTaskNotFoundException(String sagaMethod) {
        super("Not exist dtf task [%s] for saga method.".formatted(sagaMethod));
    }
}
