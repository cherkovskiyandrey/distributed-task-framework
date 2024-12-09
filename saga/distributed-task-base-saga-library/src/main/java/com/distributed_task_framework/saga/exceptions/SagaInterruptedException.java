package com.distributed_task_framework.saga.exceptions;

import java.util.UUID;

public class SagaInterruptedException extends SagaInternalException {

    public SagaInterruptedException(UUID sagaId) {
        super("saga=[%s] was interrupted".formatted(sagaId));
    }
}
