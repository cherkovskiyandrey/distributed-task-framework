package com.distributed_task_framework.utils;

@FunctionalInterface
public interface BiPredicateWithException<T, U, E extends Throwable> {

    boolean test(T t, U u) throws E;

}
