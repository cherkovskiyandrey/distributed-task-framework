package com.distributed_task_framework.test_service.services;

@FunctionalInterface
public interface ThreeConsumerWithThrowableArg<IN1, IN2, IN3> {

    void apply(IN1 in1, IN2 in2, IN3 in3, Throwable throwable);
}
