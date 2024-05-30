package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.test_service.exceptions.SagaExecutionException;
import com.distributed_task_framework.test_service.models.SagaTrackId;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public interface SagaFlow<T> {

    /**
     * Wait until saga is completed.
     *
     * @throws TimeoutException when default timeout exceed
     */
    void waitCompletion() throws TimeoutException, InterruptedException;

    /**
     * Wait until saga is completed with timeout.
     *
     * @param timeout how much wait for completion
     * @throws TimeoutException when timeout exceed
     */
    void waitCompletion(Duration timeout) throws TimeoutException, InterruptedException;

    /**
     * Wait and return the result of saga.
     * Saga result - the latest output of task in saga chain.
     *
     * @return the result of saga
     * @throws TimeoutException when default timeout exceed
     */
    Optional<T> get() throws TimeoutException, SagaExecutionException, InterruptedException;

    /**
     * Wait and return the result of saga with timeout.
     * Saga result - the latest output of task in saga chain.
     *
     * @param timeout how much wait for completion
     * @return the result of saga
     * @throws TimeoutException when default timeout exceed
     */
    Optional<T> get(Duration timeout) throws TimeoutException, SagaExecutionException, InterruptedException;

    /**
     * Check whether saga is completed or not.
     *
     * @return
     */
    boolean isCompleted();

    /**
     * Return a trackId in order to poll saga completion later.
     *
     * @return trackId for saga
     */
    SagaTrackId trackId();
}
