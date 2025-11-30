package com.distributed_task_framework.saga.functions;

import java.io.Serializable;

@FunctionalInterface
public interface SagaBiConsumer<T, U> extends Serializable {
    void accept(T t, U u) throws Exception;
}
