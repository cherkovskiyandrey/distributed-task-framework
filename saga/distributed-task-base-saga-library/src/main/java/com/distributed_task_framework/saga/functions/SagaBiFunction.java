package com.distributed_task_framework.saga.functions;

import java.io.Serializable;

@FunctionalInterface
public interface SagaBiFunction<T, U, R> extends Serializable {
    R apply(T t, U u) throws Exception;
}
