package com.distributed_task_framework.test_service.exceptions;

public class SagaMethodDuplicateException extends SagaException{

    public SagaMethodDuplicateException(String taskName) {
        super("Find saga method duplication in spring context: [%s]".formatted(taskName));
    }
}
