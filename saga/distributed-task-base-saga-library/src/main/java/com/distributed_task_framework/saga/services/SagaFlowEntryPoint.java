package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.functions.SagaRevertibleConsumer;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.functions.SagaRevertibleBiConsumer;

import java.util.function.Consumer;

public interface SagaFlowEntryPoint {

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
        SagaFunction<INPUT, OUTPUT> operation,
        SagaRevertibleBiConsumer<INPUT, OUTPUT> revertOperation,
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
        SagaFunction<INPUT, OUTPUT> operation,
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
        SagaRevertibleConsumer<INPUT> revertOperation,
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
