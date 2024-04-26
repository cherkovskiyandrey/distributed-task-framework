package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.test_service.exceptions.SagaExecutionException;

@FunctionalInterface
public interface RevertibleConsumer<IN> {

    void apply(IN in, SagaExecutionException throwable);
}
