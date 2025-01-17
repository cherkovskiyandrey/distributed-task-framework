package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.functions.SagaConsumer;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.functions.SagaRevertibleBiConsumer;
import com.distributed_task_framework.saga.functions.SagaRevertibleConsumer;

public interface SagaFlowBuilderWithoutInput<ROOT_INPUT> {

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(
        SagaConsumer<ROOT_INPUT> operation,
        SagaRevertibleConsumer<ROOT_INPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(
        SagaConsumer<ROOT_INPUT> operation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @return {@link SagaFlowBuilder}
     */
    <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(
        SagaFunction<ROOT_INPUT, OUTPUT> operation,
        SagaRevertibleBiConsumer<ROOT_INPUT, OUTPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @return {@link SagaFlowBuilder}
     */
    <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(
        SagaFunction<ROOT_INPUT, OUTPUT> operation
    );

    /**
     * Start configured saga to execute.
     *
     * @return
     */
    SagaFlowWithoutResult start();
}
