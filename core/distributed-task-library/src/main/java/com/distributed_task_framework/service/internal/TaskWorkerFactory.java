package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;

public interface TaskWorkerFactory {
    TaskWorker buildTaskWorker(TaskEntity taskEntity, TaskSettings taskSettings);
}
