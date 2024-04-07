package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.test_service.models.SagaContext;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaRegister {

    <IN, OUT> TaskDef<SagaContext> resolve(Function<IN, OUT> operation);

    <IN> TaskDef<SagaContext> resolve(Consumer<IN> operation);

    <T, U, R> TaskDef<SagaContext> resolve(BiFunction<T, U, R> operation);
}
