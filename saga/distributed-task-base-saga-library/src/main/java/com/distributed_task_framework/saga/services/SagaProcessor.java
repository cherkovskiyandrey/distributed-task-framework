package com.distributed_task_framework.saga.services;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaProcessor {

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

    /**
     * Get flow by trackId if exists.
     *
     * @param trackId       trackId for saga flow
     * @param trackingClass class for output result
     * @param <OUTPUT>      output type
     * @return {@link SagaFlow} if exists, empty otherwise
     */
    <OUTPUT> SagaFlow<OUTPUT> getFlow(UUID trackId, Class<OUTPUT> trackingClass);

    /**
     * Get flow by trackId if exists.
     *
     * @param trackId trackId for saga flow
     * @return {@link SagaFlowWithoutResult} if exists, empty otherwise
     */
    SagaFlowWithoutResult getFlow(UUID trackId);
}
