package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;

@FunctionalInterface
public interface RevertibleConsumer<IN> {

    void apply(IN in, SagaExecutionException throwable);
}
