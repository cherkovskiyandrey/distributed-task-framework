package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaOperation;
import com.distributed_task_framework.saga.models.SagaPipelineContext;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaRegister {

    <IN, OUT> SagaOperation resolve(Function<IN, OUT> operation);

    <IN> SagaOperation resolve(Consumer<IN> operation);

    <T, U, R> SagaOperation resolve(BiFunction<T, U, R> operation);

    <PARENT_INPUT, OUTPUT> SagaOperation resolveRevert(RevertibleBiConsumer<PARENT_INPUT, OUTPUT> revertOperation);

    <INPUT, PARENT_INPUT, OUTPUT> SagaOperation resolveRevert(
            RevertibleThreeConsumer<PARENT_INPUT, INPUT, OUTPUT> revertOperation
    );

    TaskDef<SagaPipelineContext> resolveByTaskName(String taskName);
}
