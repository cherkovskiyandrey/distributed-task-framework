package com.distributed_task_framework.test_service.tasks.metadata;

import com.distributed_task_framework.interceptor.TaskCreationInterceptor;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Adds metadata to every task it is attached to (see MetadataExampleTask).
 */
@Component
public class TraceIdCreationInterceptor implements TaskCreationInterceptor {

    @Override
    public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
        return executionContext.withMetadata(
            executionContext.getMetadata()
                .withIfAbsent("traceId", UUID.randomUUID().toString())
                .add("intercepted", "true")
        );
    }
}
