package com.distributed_task_framework.interceptor;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.Metadata;
import com.distributed_task_framework.model.TaskDef;
import org.springframework.lang.NonNull;

/**
 * Interceptor invoked at schedule time before the task is persisted.
 * <p>
 * Implementations have to be registered as Spring beans and can be attached to tasks
 * by class (an interface or a base class can be used as well)
 * via yaml or {@code @TaskCreationInterceptors} annotation, or as defaults for all tasks.
 * <p>
 * A thrown exception aborts scheduling and the transaction is rolled back.
 */
public interface TaskCreationInterceptor {
    /**
     * The returned execution context is used to build the task,
     * so the interceptor can add metadata (e.g. via {@link ExecutionContext#withMetadata(Metadata)}
     * and {@link Metadata#add(String, String)}).
     */
    @NonNull
    <U> ExecutionContext<U> onTaskCreated(@NonNull ExecutionContext<U> executionContext,
                                          @NonNull TaskDef<U> taskDef) throws Exception;
}
