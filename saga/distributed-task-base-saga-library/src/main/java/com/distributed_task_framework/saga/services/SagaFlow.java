package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public interface SagaFlow<T> {

    /**
     * Wait until saga is completed.
     *
     * @throws TimeoutException when default timeout exceed
     */
    void waitCompletion() throws SagaNotFoundException, InterruptedException, TimeoutException;

    /**
     * Wait until saga is completed with timeout.
     *
     * @param timeout how much wait for completion
     * @throws TimeoutException when timeout exceed
     */
    void waitCompletion(Duration timeout) throws SagaNotFoundException, InterruptedException, TimeoutException;

    /**
     * Wait and return the result of saga.
     * Saga result - the latest output of task in saga chain.
     *
     * @return the result of saga
     * @throws TimeoutException when default timeout exceed
     */
    Optional<T> get() throws SagaNotFoundException, SagaExecutionException, InterruptedException, TimeoutException;

    /**
     * Wait and return the result of saga with timeout.
     * Saga result - the latest output of task in saga chain.
     *
     * @param timeout how much wait for completion
     * @return the result of saga
     * @throws TimeoutException when default timeout exceed
     */
    Optional<T> get(Duration timeout) throws SagaNotFoundException, SagaExecutionException, InterruptedException, TimeoutException;

    /**
     * Check whether saga is completed or not.
     *
     * @return
     */
    boolean isCompleted() throws SagaNotFoundException;

    /**
     * Return a trackId in order to poll saga completion later.
     *
     * @return trackId for saga
     */
    UUID trackId();
}
