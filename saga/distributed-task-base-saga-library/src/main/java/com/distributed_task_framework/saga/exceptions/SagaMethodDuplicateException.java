package com.distributed_task_framework.saga.exceptions;

public class SagaMethodDuplicateException extends SagaInternalException {

    public SagaMethodDuplicateException(String taskName) {
        super("Find saga method duplication in spring context: [%s]".formatted(taskName));
    }
}
