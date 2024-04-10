package com.distributed_task_framework.test_service.exceptions;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.test_service.annotations.SagaMethod;

public class SagaMethodResolvingException extends SagaException {

    public SagaMethodResolvingException(String operation) {
        super("Operation [%s] doesn't reference to [%s] method".formatted(operation, SagaMethod.class.getSimpleName()));
    }

    public SagaMethodResolvingException(TaskDef<?> taskDef) {
        super("Couldn't find saga method for task [%s]".formatted(taskDef));
    }
}
