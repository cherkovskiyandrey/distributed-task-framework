package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.test_service.models.SagaPipelineContext;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaRegister {

    <IN, OUT> TaskDef<SagaPipelineContext> resolve(Function<IN, OUT> operation);

    <IN> TaskDef<SagaPipelineContext> resolve(Consumer<IN> operation);

    <T, U, R> TaskDef<SagaPipelineContext> resolve(BiFunction<T, U, R> operation);

    <PARENT_INPUT, OUTPUT> TaskDef<SagaPipelineContext> resolveRevert(
            RevertibleBiConsumer<PARENT_INPUT, OUTPUT> revertOperation
    );

    <INPUT, PARENT_INPUT, OUTPUT> TaskDef<SagaPipelineContext> resolveRevert(
            RevertibleThreeConsumer<PARENT_INPUT, INPUT, OUTPUT> revertOperation
    );

    TaskDef<SagaPipelineContext> resolveByTaskName(String taskName);
}
