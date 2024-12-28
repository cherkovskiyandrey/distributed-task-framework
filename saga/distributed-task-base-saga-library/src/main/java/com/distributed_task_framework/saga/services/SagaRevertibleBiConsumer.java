package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;

import java.io.Serializable;

@FunctionalInterface
public interface SagaRevertibleBiConsumer<IN1, IN2> extends Serializable {

    void apply(IN1 in1, IN2 in2, SagaExecutionException exception);
}
