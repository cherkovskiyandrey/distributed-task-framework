package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.functions.SagaBiConsumer;
import com.distributed_task_framework.saga.functions.SagaBiFunction;
import com.distributed_task_framework.saga.functions.SagaConsumer;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.functions.SagaRevertibleBiConsumer;
import com.distributed_task_framework.saga.functions.SagaRevertibleConsumer;
import com.distributed_task_framework.saga.functions.SagaRevertibleThreeConsumer;

public interface SagaFlowBuilder<ROOT_INPUT, PARENT_OUTPUT> {

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @param <OUTPUT>        output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(
        SagaBiFunction<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> operation,
        SagaRevertibleThreeConsumer<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @param <OUTPUT>  output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(
        SagaBiFunction<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> operation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @param <OUTPUT>        output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(
        SagaFunction<PARENT_OUTPUT, OUTPUT> operation,
        SagaRevertibleBiConsumer<PARENT_OUTPUT, OUTPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @param <OUTPUT>  output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(
        SagaFunction<PARENT_OUTPUT, OUTPUT> operation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(
        SagaBiConsumer<PARENT_OUTPUT, ROOT_INPUT> operation,
        SagaRevertibleBiConsumer<PARENT_OUTPUT, ROOT_INPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(
        SagaBiConsumer<PARENT_OUTPUT, ROOT_INPUT> operation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(
        SagaConsumer<PARENT_OUTPUT> operation,
        SagaRevertibleConsumer<PARENT_OUTPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(
        SagaConsumer<PARENT_OUTPUT> operation
    );

    /**
     * Start configured saga to execute.
     *
     * @return
     */
    SagaFlow<PARENT_OUTPUT> start();
}
