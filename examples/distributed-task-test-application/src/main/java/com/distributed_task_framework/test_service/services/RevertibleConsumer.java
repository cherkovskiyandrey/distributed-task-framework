package com.distributed_task_framework.test_service.services;

@FunctionalInterface
public interface RevertibleConsumer<IN> {

    void apply(IN in, Throwable throwable);
}
