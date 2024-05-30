package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;

@FunctionalInterface
public interface RevertibleThreeConsumer<IN1, IN2, IN3> {

    void apply(IN1 in1, IN2 in2, IN3 in3, SagaExecutionException exception);
}
