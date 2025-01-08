package com.distributed_task_framework.saga.exceptions;

public class SagaMethodDuplicateException extends SagaInternalException {

    public SagaMethodDuplicateException(String name) {
        super("Find saga method duplication: [%s]".formatted(name));
    }
}
