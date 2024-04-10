package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.test_service.models.SagaRevert;
import com.distributed_task_framework.test_service.models.SagaRevertInputOnly;

import java.util.function.Consumer;
import java.util.function.Function;

public interface SagaFlowBuilderWithoutInput {

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
            Consumer<INPUT> operation,
            Consumer<SagaRevertInputOnly<INPUT>> revertOperation,
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
            Consumer<INPUT> operation,
            INPUT input
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
    <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(
            Function<INPUT, OUTPUT> operation,
            Consumer<SagaRevert<INPUT, OUTPUT>> revertOperation,
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
    <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> thenRun(
            Function<INPUT, OUTPUT> operation,
            INPUT input
    );

    SagaFlowWithoutResult start();

    SagaFlowWithoutResult startWithAffinity(String affinityGroup, String affinity);
}