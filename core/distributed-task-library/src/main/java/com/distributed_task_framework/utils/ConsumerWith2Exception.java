package com.distributed_task_framework.utils;

@FunctionalInterface
public interface ConsumerWith2Exception<T, E1 extends Throwable, E2 extends Throwable> {

    void accept(T t) throws E1, E2;
}
