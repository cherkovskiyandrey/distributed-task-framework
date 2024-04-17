package com.distributed_task_framework.test_service.services;

@FunctionalInterface
public interface BiConsumerWithThrowableArg<IN1, IN2> {

    void apply(IN1 in1, IN2 in2, Throwable throwable);
}
