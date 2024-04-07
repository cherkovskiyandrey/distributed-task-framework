package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.test_service.models.SagaRevert;
import com.distributed_task_framework.test_service.models.SagaRevertInputOnly;
import com.distributed_task_framework.test_service.models.SagaRevertWithParentInput;
import com.distributed_task_framework.test_service.models.SagaRevertWithParentInputOnly;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaFlowBuilder<PARENT_INPUT> {

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @param input           input argument which will be serializable and passed into operation
     * @param <INPUT>         input type of operation
     * @param <OUTPUT>        output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(
            BiFunction<PARENT_INPUT, INPUT, OUTPUT> operation,
            Consumer<SagaRevertWithParentInput<PARENT_INPUT, INPUT, OUTPUT>> revertOperation,
            INPUT input
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @param input     input argument which will be serializable and passed into operation
     * @param <INPUT>   input type of operation
     * @param <OUTPUT>  output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(
            BiFunction<PARENT_INPUT, INPUT, OUTPUT> operation,
            INPUT input
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @param <OUTPUT>        output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(
            Function<PARENT_INPUT, OUTPUT> operation,
            Consumer<SagaRevert<PARENT_INPUT, OUTPUT>> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @param <OUTPUT>  output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(
            Function<PARENT_INPUT, OUTPUT> operation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @param input           input argument which will be serializable and passed into operation
     * @param <INPUT>         input type of operation
     * @return {@link SagaFlowBuilder}
     */
    <INPUT> SagaFlowBuilderWithoutInput thenConsume(
            BiConsumer<PARENT_INPUT, INPUT> operation,
            Consumer<SagaRevertWithParentInputOnly<PARENT_INPUT, INPUT>> revertOperation,
            INPUT input
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @param input     input argument which will be serializable and passed into operation
     * @param <INPUT>   input type of operation
     * @return {@link SagaFlowBuilder}
     */
    <INPUT> SagaFlowBuilderWithoutInput thenConsume(
            BiConsumer<PARENT_INPUT, INPUT> operation,
            INPUT input
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput thenConsume(
            Consumer<PARENT_INPUT> operation,
            Consumer<SagaRevertInputOnly<PARENT_INPUT>> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput thenConsume(
            Consumer<PARENT_INPUT> operation
    );

    SagaFlow<PARENT_INPUT> start();

    SagaFlow<PARENT_INPUT> startWithAffinity(String affinityGroup, String affinity);
}
