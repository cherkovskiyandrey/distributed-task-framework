package com.distributed_task_framework.saga.functions;

import java.io.Serializable;
import java.util.function.BiConsumer;

@FunctionalInterface
public interface SagaBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {
}
