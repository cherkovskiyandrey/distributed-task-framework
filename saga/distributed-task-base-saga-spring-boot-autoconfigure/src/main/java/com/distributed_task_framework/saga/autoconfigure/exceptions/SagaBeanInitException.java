package com.distributed_task_framework.saga.autoconfigure.exceptions;

import com.distributed_task_framework.saga.exceptions.SagaException;

public class SagaBeanInitException extends SagaException {
    public SagaBeanInitException(String message) {
        super(message);
    }
}
