package com.distributed_task_framework.saga.autoconfigure.services.impl;

import com.distributed_task_framework.saga.autoconfigure.annotations.SagaMethod;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaRevertMethod;
import com.distributed_task_framework.saga.autoconfigure.services.ExternalSagaTestService;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import org.jetbrains.annotations.Nullable;
import org.springframework.stereotype.Service;

@Service
public class ExternalSagaTestServiceImpl implements ExternalSagaTestService {
    private String value = "";

    @SagaMethod(name = "external-forward")
    @Override
    public String forward(String val) {
        value += ("+" + val);
        throw new RuntimeException();
    }

    @SagaRevertMethod(name = "external-backward")
    @Override
    public void backward(String val, @Nullable String output, @Nullable SagaExecutionException sagaExecutionException) {
        value += ("-" + val);
    }

    @Override
    public void setValue(String val) {
        value = val;
    }

    @Override
    public String getValue() {
        return value;
    }
}
