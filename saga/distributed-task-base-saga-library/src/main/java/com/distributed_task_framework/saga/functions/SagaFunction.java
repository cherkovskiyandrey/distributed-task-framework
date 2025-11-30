package com.distributed_task_framework.saga.functions;

import java.io.Serializable;

@FunctionalInterface
public interface SagaFunction<T, R> extends Serializable {
    R apply(T t) throws Exception;
}
