package com.distributed_task_framework.saga.services;

import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaFlowBuilderWithoutInput<ROOT_INPUT> {

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
            Consumer<ROOT_INPUT> operation,
            RevertibleConsumer<ROOT_INPUT> revertOperation
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
            Consumer<ROOT_INPUT> operation
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
    <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(
            Function<ROOT_INPUT, OUTPUT> operation,
            SagaRevertibleBiConsumer<ROOT_INPUT, OUTPUT> revertOperation
    );

    /**
     * Allow to set next operation in current saga.
     *
     * @param operation saga operation
     * @param input     input argument which will be serializable and passed into operation
     * @param <INPUT>   input type of operation
     * @return {@link SagaFlowBuilder}
     */
    <OUTPUT> SagaFlowBuilder<ROOT_INPUT, OUTPUT> thenRun(
            Function<ROOT_INPUT, OUTPUT> operation
    );

    SagaFlowWithoutResult start();

    SagaFlowWithoutResult startWithAffinity(String affinityGroup, String affinity);
}
