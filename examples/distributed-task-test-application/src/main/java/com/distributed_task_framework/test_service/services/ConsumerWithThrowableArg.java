package com.distributed_task_framework.test_service.services;

@FunctionalInterface
public interface ConsumerWithThrowableArg<IN> {

    void apply(IN in, Throwable throwable);
}
