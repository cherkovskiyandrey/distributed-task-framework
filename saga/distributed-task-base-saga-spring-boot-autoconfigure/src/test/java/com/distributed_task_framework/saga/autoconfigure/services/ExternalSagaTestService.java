package com.distributed_task_framework.saga.autoconfigure.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import jakarta.annotation.Nullable;

public interface ExternalSagaTestService {

    String forward(String val);

    void backward(String val, @Nullable String output, @Nullable SagaExecutionException sagaExecutionException);

    void setValue(String val);

    String getValue();
}
