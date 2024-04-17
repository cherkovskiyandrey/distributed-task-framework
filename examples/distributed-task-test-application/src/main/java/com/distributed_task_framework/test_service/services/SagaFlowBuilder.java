package com.distributed_task_framework.test_service.services;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaFlowBuilder<PARENT_OUTPUT> {

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
            BiFunction<PARENT_OUTPUT, INPUT, OUTPUT> operation,
            ThreeConsumerWithThrowableArg<PARENT_OUTPUT, INPUT, OUTPUT> revertOperation,
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
            BiFunction<PARENT_OUTPUT, INPUT, OUTPUT> operation,
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
            Function<PARENT_OUTPUT, OUTPUT> operation,
            BiConsumerWithThrowableArg<PARENT_OUTPUT, OUTPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @param <OUTPUT>  output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(
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
    <INPUT> SagaFlowBuilderWithoutInput thenConsume(
            BiConsumer<PARENT_OUTPUT, INPUT> operation,
            BiConsumerWithThrowableArg<PARENT_OUTPUT, INPUT> revertOperation,
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
            BiConsumer<PARENT_OUTPUT, INPUT> operation,
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
            Consumer<PARENT_OUTPUT> operation,
            ConsumerWithThrowableArg<PARENT_OUTPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @return {@link SagaFlowBuilder}
     */
    SagaFlowBuilderWithoutInput thenConsume(
            Consumer<PARENT_OUTPUT> operation
    );

    SagaFlow<PARENT_OUTPUT> start();

    SagaFlow<PARENT_OUTPUT> startWithAffinity(String affinityGroup, String affinity);
}
