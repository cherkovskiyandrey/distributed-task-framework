package com.distributed_task_framework.saga.exceptions;

import java.lang.reflect.Method;

public class SagaMethodDuplicateException extends SagaInternalException {

    public SagaMethodDuplicateException(String name) {
        super("Find saga method duplication: name=[%s]".formatted(name));
    }

    public SagaMethodDuplicateException(Method method) {
        super("Find saga method duplication: method=[%s]".formatted(method));
    }
}
