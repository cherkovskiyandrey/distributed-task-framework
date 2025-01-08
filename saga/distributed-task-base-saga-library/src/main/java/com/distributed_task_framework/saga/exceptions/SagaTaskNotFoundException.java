package com.distributed_task_framework.saga.exceptions;

public class SagaTaskNotFoundException extends SagaInternalException {

    //todo: move spec message to client level
    public SagaTaskNotFoundException(String sagaMethod) {
        super("Not exist dtf task [%s] for saga method.".formatted(sagaMethod));
    }
}
