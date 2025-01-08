package com.distributed_task_framework.test.autoconfigure.service;

import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.test.autoconfigure.exception.FailedCancellationException;

import java.time.Duration;

/**
 * Helper bean in order to manage dtf in tests.
 */
public interface DistributedTaskTestUtil {

    /**
     * Prepare dtf after test and before new one.
     * Contains next actions:
     * <ol>
     *     <li>Cancel all current tasks.</li>
     *     <li>Wait for completion for all canceled tasks.</li>
     *     <li>Wait for all tasks in {@link VirtualQueue} == {@link VirtualQueue#DELETED} are processed and removed</li>
     * </ol>
     * @throws FailedCancellationException in case can't cancel any task
     * @throws InterruptedException if any thread has interrupted the current thread.
     * The interrupted status of the current thread is cleared when this exception is thrown
     */
    void reinitAndWait() throws InterruptedException;

    /**
     * Prepare dtf after test and before new one.
     * Contains next actions:
     * <ol>
     *     <li>Cancel all current tasks.</li>
     *     <li>Wait for completion for all canceled tasks.</li>
     *     <li>Wait for all tasks in {@link VirtualQueue} == {@link VirtualQueue#DELETED} are processed and removed</li>
     * </ol>
     * @param attemptsToCancel how many attempt to make in order to cancel all tasks
     * @param duration how much time wait for each workflow is completed after cancellation
     * @throws FailedCancellationException in case can't cancel any task
     * @throws InterruptedException if any thread has interrupted the current thread.
     * The interrupted status of the current thread is cleared when this exception is thrown
     */
    void reinitAndWait(int attemptsToCancel, Duration duration) throws InterruptedException;
}
