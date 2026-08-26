package com.distributed_task_framework.interceptor;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import org.springframework.lang.NonNull;

/**
 * Around-style interceptor invoked around task execution.
 * <p>
 * Implementations have to be registered as Spring beans and can be attached to tasks
 * by class (an interface or a base class can be used as well)
 * via yaml or {@code @TaskExecutionInterceptors} annotation, or as defaults for all tasks.
 * <p>
 * A thrown exception (either from the interceptor itself or from the chain) is treated
 * as an ordinary task failure: the retry policy and DLT are applied.
 */
@FunctionalInterface
public interface TaskExecutionInterceptor {
    /**
     * @param chain has to be invoked to run the next interceptor or the task itself
     */
    <U> void execute(@NonNull ExecutionContext<U> executionContext,
                     @NonNull TaskDef<U> taskDef,
                     @NonNull TaskExecutionChain chain) throws Exception;
}
