package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.WorkerContext;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import com.distributed_task_framework.service.internal.WorkerContextManager;

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
    public void cleanCurrentContext() {
        currentExecutionContext.remove();
    }
}
