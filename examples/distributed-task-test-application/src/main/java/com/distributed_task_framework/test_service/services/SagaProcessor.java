package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.test_service.models.SagaRevert;
import com.distributed_task_framework.test_service.models.SagaRevertInputOnly;
import com.distributed_task_framework.test_service.models.SagaTrackId;

import java.util.Optional;
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
    <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> registerToRun(
            Function<INPUT, OUTPUT> operation,
            Consumer<SagaRevert<INPUT, OUTPUT>> revertOperation,
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
    <INPUT, OUTPUT> SagaFlowBuilder<OUTPUT> registerToRun(
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
    <INPUT> SagaFlowBuilderWithoutInput registerToConsume(
            Consumer<INPUT> operation,
            Consumer<SagaRevertInputOnly<INPUT>> revertOperation,
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
    <INPUT> SagaFlowBuilderWithoutInput registerToConsume(
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
    <OUTPUT> Optional<SagaFlow<OUTPUT>> getFlow(SagaTrackId trackId, Class<OUTPUT> trackingClass);

    /**
     * Get flow by trackId if exists.
     *
     * @param trackId trackId for saga flow
     * @return {@link SagaFlowWithoutResult} if exists, empty otherwise
     */
    SagaFlowWithoutResult getFlow(SagaTrackId trackId);
}
