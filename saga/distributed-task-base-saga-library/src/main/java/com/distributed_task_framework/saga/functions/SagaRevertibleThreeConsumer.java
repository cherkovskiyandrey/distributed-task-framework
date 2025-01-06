package com.distributed_task_framework.saga.functions;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;

import java.io.Serializable;

@FunctionalInterface
public interface SagaRevertibleThreeConsumer<IN1, IN2, IN3> extends Serializable {

    void apply(IN1 in1, IN2 in2, IN3 in3, SagaExecutionException exception);
}
