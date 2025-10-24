package com.distributed_task_framework.saga.test.autoconfiguration.service;

import com.distributed_task_framework.test.autoconfigure.exception.FailedCancellationException;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;

import java.time.Duration;

/**
 * Helper bean in order to manage saga in tests.
 */
public interface SagaTestUtil {

    /**
     * Reset saga and underlined dtf after test and/or before new one.
     * Contains next actions:
     * <ol>
     *     <li>Invoke {@link DistributedTaskTestUtil#reinitAndWait()}</li>
     *     <li>Clean {@link com.distributed_task_framework.saga.persistence.entities.SagaEntity}</li>
     *     <li>Clean {@link com.distributed_task_framework.saga.persistence.entities.DlsSagaEntity}</li>
     * </ol>
     *
     * @throws FailedCancellationException in case can't cancel any task
     * @throws InterruptedException if any thread has interrupted the current thread.
     * The interrupted status of the current thread is cleared when this exception is thrown
     */
    void reinitAndWait() throws InterruptedException;

    /**
     * Reset saga and underlined dtf after test and/or before new one.
     * Contains next actions:
     * <ol>
     *     <li>Invoke {@link DistributedTaskTestUtil#reinitAndWait()}</li>
     *     <li>Clean {@link com.distributed_task_framework.saga.persistence.entities.SagaEntity}</li>
     *     <li>Clean {@link com.distributed_task_framework.saga.persistence.entities.DlsSagaEntity}</li>
     * </ol>
     *
     * @param attemptsToCancel how many attempt to make in order to cancel all underlined tasks
     * @param duration how much time wait for each underlined workflow is completed after cancellation
     * @throws FailedCancellationException in case can't cancel any task
     * @throws InterruptedException if any thread has interrupted the current thread.
     * The interrupted status of the current thread is cleared when this exception is thrown
     */
    void reinitAndWait(int attemptsToCancel, Duration duration) throws InterruptedException;
}
