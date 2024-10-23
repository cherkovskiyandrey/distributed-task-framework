package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;

@FunctionalInterface
public interface RevertibleBiConsumer<IN1, IN2> {

    void apply(IN1 in1, IN2 in2, SagaExecutionException exception);
}
