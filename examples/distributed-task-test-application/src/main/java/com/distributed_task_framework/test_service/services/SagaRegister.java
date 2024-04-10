package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.model.TaskDef;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaRegister {

    <IN, OUT, CONTEXT> TaskDef<CONTEXT> resolve(Function<IN, OUT> operation, Class<CONTEXT> contextClass);

    <IN, CONTEXT> TaskDef<CONTEXT> resolve(Consumer<IN> operation, Class<CONTEXT> contextClass);

    <T, U, R, CONTEXT> TaskDef<CONTEXT> resolve(BiFunction<T, U, R> operation, Class<CONTEXT> contextClass);
}
