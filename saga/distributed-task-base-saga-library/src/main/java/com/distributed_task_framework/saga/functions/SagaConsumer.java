package com.distributed_task_framework.saga.functions;

import java.io.Serializable;

@FunctionalInterface
public interface SagaConsumer<T> extends Serializable {
    void accept(T t) throws Exception;
}
