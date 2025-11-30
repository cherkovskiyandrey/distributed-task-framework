package com.distributed_task_framework.saga.autoconfigure.test_data.services.impl;

import com.distributed_task_framework.saga.autoconfigure.annotations.SagaMethod;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaRevertMethod;
import com.distributed_task_framework.saga.autoconfigure.test_data.services.InternalSagaBaseTestService;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.TimeoutException;

@RequiredArgsConstructor
public class InternalSagaBaseTestServiceImpl implements InternalSagaBaseTestService {
    private final DistributionSagaService distributionSagaService;
    private String value = "";

    @Override
    public void calculate(String suffix) throws InterruptedException, TimeoutException {
        distributionSagaService.create("test-internal")
            .registerToRun(this::forward, this::backward, suffix)
            .start()
            .waitCompletion();
    }

    @SagaMethod(name = "internal-sum")
    private String forward(String val) {
        value += ("+" + val);
        throw new RuntimeException();
    }

    @SagaRevertMethod(name = "internal-diff")
    private void backward(String val, @Nullable String output, @Nullable SagaExecutionException sagaExecutionException) {
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
