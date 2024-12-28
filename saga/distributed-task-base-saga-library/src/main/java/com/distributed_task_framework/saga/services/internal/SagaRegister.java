package com.distributed_task_framework.saga.services.internal;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaOperation;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.SagaRevertibleBiConsumer;
import com.distributed_task_framework.saga.services.SagaRevertibleThreeConsumer;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaRegister {

    <IN, OUT> SagaOperation resolve(Function<IN, OUT> operation);

    <IN> SagaOperation resolve(Consumer<IN> operation);

    <T, U, R> SagaOperation resolve(BiFunction<T, U, R> operation);

    <PARENT_INPUT, OUTPUT> SagaOperation resolveRevert(SagaRevertibleBiConsumer<PARENT_INPUT, OUTPUT> revertOperation);

    <INPUT, PARENT_INPUT, OUTPUT> SagaOperation resolveRevert(
            SagaRevertibleThreeConsumer<PARENT_INPUT, INPUT, OUTPUT> revertOperation
    );

    TaskDef<SagaPipeline> resolveByTaskName(String taskName);
}
