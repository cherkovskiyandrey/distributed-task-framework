package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.WorkerContext;

import java.util.Optional;

public interface WorkerContextManager {

    Optional<WorkerContext> getCurrentContext();

    void setCurrentContext(WorkerContext currentContext);

    void cleanCurrentContext();
}
