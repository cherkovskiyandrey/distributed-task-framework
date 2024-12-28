package com.distributed_task_framework.saga.services;

import java.io.Serializable;
import java.util.function.BiFunction;

@FunctionalInterface
public interface SagaBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {
}
