package com.distributed_task_framework.saga.functions;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;

import java.io.Serializable;

@FunctionalInterface
public interface SagaRevertibleConsumer<IN> extends Serializable {
    void apply(IN in, SagaExecutionException throwable) throws Exception;
}
