package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.test_service.exceptions.SagaExecutionException;

@FunctionalInterface
public interface RevertibleBiConsumer<IN1, IN2> {

    void apply(IN1 in1, IN2 in2, SagaExecutionException exception);
}
