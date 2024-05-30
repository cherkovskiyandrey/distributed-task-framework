package com.distributed_task_framework.utils;

@FunctionalInterface
public interface ConsumerWithException<T, E extends Throwable> {

    void accept(T t) throws E;
}
