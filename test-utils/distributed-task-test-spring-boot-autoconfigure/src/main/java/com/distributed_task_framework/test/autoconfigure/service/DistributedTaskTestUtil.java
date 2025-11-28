package com.distributed_task_framework.test.autoconfigure.service;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.test.autoconfigure.exception.FailedCancellationException;

import java.time.Duration;
import java.util.List;

/**
 * Helper bean in order to manage dtf in tests.
 */
public interface DistributedTaskTestUtil {

    /**
     * See {@link DistributedTaskTestUtil#reinitAndWait(int, Duration, List)}
     */
    void reinitAndWait() throws InterruptedException, FailedCancellationException;

    /**
     * See {@link DistributedTaskTestUtil#reinitAndWait(int, Duration, List)}
     */
    void reinitAndWait(int attemptsToCancel, Duration duration) throws InterruptedException, FailedCancellationException;

    /**
     * Reset dtf after test and/or before new one.
     * Contains next actions:
     * <ol>
     *     <li>Cancel all current tasks.</li>
     *     <li>Wait for completion for all canceled tasks.</li>
     *     <li>Wait for all tasks in {@link VirtualQueue} == {@link VirtualQueue#DELETED} are processed and removed</li>
     * </ol>
     * @param attemptsToCancel how many attempt to make in order to cancel all tasks
     * @param duration how much time wait for each workflow is completed after cancellation
     * @param excludeList list of task definitions to skip reinit for
     * @throws FailedCancellationException in case can't cancel any task
     * @throws InterruptedException if any thread has interrupted the current thread.
     * The interrupted status of the current thread is cleared when this exception is thrown
     */
    void reinitAndWait(int attemptsToCancel,
                       Duration duration,
                       List<TaskDef<?>> excludeList) throws InterruptedException, FailedCancellationException;
}
