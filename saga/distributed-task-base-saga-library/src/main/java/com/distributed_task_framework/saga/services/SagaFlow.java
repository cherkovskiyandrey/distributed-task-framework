package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaCancellationException;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public interface SagaFlow<T> {

    /**
     * Wait until saga is completed (including revert flow in case of error or graceful cancellation).
     *
     * @throws SagaNotFoundException if saga doesn't exist or completed and was removed by timeout
     * @throws InterruptedException  if any thread has interrupted the current thread
     * @throws TimeoutException      if default timeout exceed
     */
    void waitCompletion() throws SagaNotFoundException, InterruptedException, TimeoutException;

    /**
     * Wait until saga is completed (including revert flow in case of error or graceful cancellation) with timeout.
     *
     * @throws SagaNotFoundException if saga doesn't exist or completed and was removed by timeout
     * @throws InterruptedException  if any thread has interrupted the current thread
     * @throws TimeoutException      if timeout exceed
     */
    void waitCompletion(Duration timeout) throws SagaNotFoundException, InterruptedException, TimeoutException;

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

    /**
     * Check whether saga is completed or not (including revert flow in case of error or graceful cancellation).
     *
     * @return
     * @throws SagaNotFoundException if saga doesn't exist or completed and was removed by timeout
     */
    boolean isCompleted() throws SagaNotFoundException;

    /**
     * Check whether saga is gracefully canceled or not.
     *
     * @return
     * @throws SagaNotFoundException if saga doesn't exist or completed and was removed by timeout
     */
    boolean isCanceled() throws SagaNotFoundException;

    /**
     * Return a trackId in order to poll saga completion or cancel later.
     *
     * @return trackId for saga
     */
    UUID trackId();

    /**
     * Cancel saga.
     * If flag "gracefully" is true:
     * <ol>
     *     <li>If saga in progress, it will wait for completion of current operation and start revert flow</li>
     *     <li>If saga in progress of revert flow, this command do nothing</li>
     * </ol>
     * If flag "gracefully" is false: cancel saga immediately. Means:
     * <ol>
     *     <li>Interrupt thread which execute of current saga step or revert step</li>
     *     <li>Prevent to run revert flow</li>
     * </ol>
     *
     * @param gracefully
     */
    void cancel(boolean gracefully) throws SagaNotFoundException;
}
