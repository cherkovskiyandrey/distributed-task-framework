package com.distributed_task_framework.saga.functions;

import java.io.Serializable;
import java.util.function.Consumer;

@FunctionalInterface
public interface SagaConsumer<T> extends Consumer<T>, Serializable {
}
