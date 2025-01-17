package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaCancellationException;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public interface SagaFlow<T> extends SagaFlowWithoutResult {

    /**
     * Wait and return the result of saga.
     * Saga result - the latest output of task in saga chain.
     *
     * @return the result of saga
     * @throws SagaNotFoundException     if saga doesn't exist or completed and was removed by timeout
     * @throws SagaExecutionException    if any task (exclude rollback) threw an exception, contains original caused exception
     * @throws InterruptedException      if any thread has interrupted the current thread
     * @throws TimeoutException          if default timeout exceed
     * @throws SagaCancellationException if the computation was cancelled
     */
    Optional<T> get() throws
        SagaNotFoundException,
        SagaExecutionException,
        InterruptedException,
        TimeoutException,
        SagaCancellationException;

    /**
     * Wait and return the result of saga with timeout.
     * Saga result - the latest output of task in saga chain.
     *
     * @param timeout how much wait for completion
     * @return the result of saga
     * @throws SagaNotFoundException     if saga doesn't exist or completed and was removed by timeout
     * @throws SagaExecutionException    if any task (exclude rollback) threw an exception, contains original caused exception
     * @throws InterruptedException      if any thread has interrupted the current thread
     * @throws TimeoutException          if timeout exceed
     * @throws SagaCancellationException if the computation was cancelled
     */
    Optional<T> get(Duration timeout) throws SagaNotFoundException,
        SagaExecutionException,
        InterruptedException,
        TimeoutException,
        SagaCancellationException;
}
