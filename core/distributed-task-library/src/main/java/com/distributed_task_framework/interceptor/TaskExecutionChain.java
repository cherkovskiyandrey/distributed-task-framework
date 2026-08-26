package com.distributed_task_framework.interceptor;

/**
 * Chain of execution interceptors ended with the task itself.
 */
@FunctionalInterface
public interface TaskExecutionChain {
    /**
     * Continue execution: invoke the next interceptor or the task.
     */
    void proceed() throws Exception;
}
