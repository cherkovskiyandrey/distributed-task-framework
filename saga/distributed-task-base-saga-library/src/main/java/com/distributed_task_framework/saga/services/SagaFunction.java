package com.distributed_task_framework.saga.services;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface SagaFunction<T, R> extends Function<T, R>, Serializable {
}
