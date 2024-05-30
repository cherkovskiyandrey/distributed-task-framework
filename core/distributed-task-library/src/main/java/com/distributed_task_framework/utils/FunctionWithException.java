package com.distributed_task_framework.utils;

@FunctionalInterface
public interface FunctionWithException<R, T, E extends Throwable> {

    R call(T t) throws E;
}
