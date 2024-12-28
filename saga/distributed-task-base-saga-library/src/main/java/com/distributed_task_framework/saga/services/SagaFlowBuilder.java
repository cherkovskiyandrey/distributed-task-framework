package com.distributed_task_framework.saga.services;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaFlowBuilder<ROOT_INPUT, PARENT_OUTPUT> {

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
    <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(
        SagaBiFunction<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> operation,
        SagaRevertibleThreeConsumer<PARENT_OUTPUT, ROOT_INPUT, OUTPUT> revertOperation
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
        Function<PARENT_OUTPUT, OUTPUT> operation,
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
        Function<PARENT_OUTPUT, OUTPUT> operation
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
    SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(
        BiConsumer<PARENT_OUTPUT, ROOT_INPUT> operation,
        SagaRevertibleBiConsumer<PARENT_OUTPUT, ROOT_INPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @param input     input argument which will be serializable and passed into operation
     * @param <INPUT>   input type of operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(
        BiConsumer<PARENT_OUTPUT, ROOT_INPUT> operation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(
        Consumer<PARENT_OUTPUT> operation,
        RevertibleConsumer<PARENT_OUTPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput<ROOT_INPUT> thenConsume(
        Consumer<PARENT_OUTPUT> operation
    );

    SagaFlow<PARENT_OUTPUT> start();
}
