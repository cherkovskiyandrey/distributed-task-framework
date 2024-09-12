package com.distributed_task_framework.saga.services;

import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaEntryPoint {

    /**
     * Entry point to build saga transaction.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @param input           input argument which will be serializable and passed into operation
     * @param <INPUT>         input type of operation
     * @param <OUTPUT>        output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> registerToRun(
            Function<INPUT, OUTPUT> operation,
            RevertibleBiConsumer<INPUT, OUTPUT> revertOperation,
            INPUT input
    );


    /**
     * Entry point to build saga transaction.
     *
     * @param operation saga operation
     * @param input     input argument which will be serializable and passed into operation
     * @param <INPUT>   input type of operation
     * @param <OUTPUT>  output type of operation
     * @return {@link SagaFlowBuilder}
     */
    <INPUT, OUTPUT> SagaFlowBuilder<INPUT, OUTPUT> registerToRun(
            Function<INPUT, OUTPUT> operation,
            INPUT input
    );

    /**
     * Entry point to build saga transaction.
     *
     * @param operation       saga operation
     * @param revertOperation saga revert operation
     * @param input           input argument which will be serializable and passed into operation
     * @param <INPUT>         input type of operation
     * @return {@link SagaFlowBuilder}
     */
    <INPUT> SagaFlowBuilderWithoutInput<INPUT> registerToConsume(
            Consumer<INPUT> operation,
            RevertibleConsumer<INPUT> revertOperation,
            INPUT input
    );

    /**
     * Entry point to build saga transaction.
     *
     * @param operation saga operation
     * @param input     input argument which will be serializable and passed into operation
     * @param <INPUT>   input type of operation
     * @return {@link SagaFlowBuilder}
     */
    <INPUT> SagaFlowBuilderWithoutInput<INPUT> registerToConsume(
            Consumer<INPUT> operation,
            INPUT input
    );
}
