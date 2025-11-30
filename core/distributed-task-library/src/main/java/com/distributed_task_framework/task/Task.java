package com.distributed_task_framework.task;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;

/**
 * Base interface for simple general task.
 * Is applicable for general cases.
 *
 * @param <T>
 */
public interface Task<T> {

    /**
     * @return task definition
     */
    TaskDef<T> getDef();

    /**
     * Business logic of task.
     *
     * @param executionContext
     */
    default void execute(ExecutionContext<T> executionContext) throws Exception {
    }


    /**
     * Callback is invoked on failure cases in the execution.
     *
     * @param failedExecutionContext
     */
    default void onFailure(FailedExecutionContext<T> failedExecutionContext) throws Exception {
    }

    /**
     * Callback is invoked on failure cases in the execution with result.
     *
     * @param failedExecutionContext
     * @return retry interruption: true - ask framework to interrupt
     * retrying of current task. Usually it means that error is unrecoverable, and it doesn't make sense
     * to retry this task according to retry policy, false - retry for current task according to current retry policy
     */
    default boolean onFailureWithResult(FailedExecutionContext<T> failedExecutionContext) throws Exception {
        onFailure(failedExecutionContext);
        return false;
    }
}
