package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.WorkerContext;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class WorkerContextManagerImpl implements WorkerContextManager {
    ThreadLocal<WorkerContext> currentExecutionContext = new ThreadLocal<>();

    /**
     * @noinspection unchecked
     */
    @Override
    public Optional<WorkerContext> getCurrentContext() {
        return Optional.ofNullable(currentExecutionContext.get());
    }

    @Override
    public void setCurrentContext(WorkerContext currentContext) {
        currentExecutionContext.set(currentContext);
    }

    @Override
    public void resetCurrentContext() {
        var currentContext = currentExecutionContext.get();
        if (currentContext == null) {
            return;
        }
        currentExecutionContext.set(
            WorkerContext.builder()
                .workflowId(currentContext.getWorkflowId())
                .workflowName(currentContext.getWorkflowName())
                .currentTaskId(currentContext.getCurrentTaskId())
                .taskSettings(currentContext.getTaskSettings())
                .taskEntity(currentContext.getTaskEntity())
                .stateHolder(currentContext.getStateHolder())
                .build()
        );
    }

    @Override
    public void cleanCurrentContext() {
        currentExecutionContext.remove();
    }
}
