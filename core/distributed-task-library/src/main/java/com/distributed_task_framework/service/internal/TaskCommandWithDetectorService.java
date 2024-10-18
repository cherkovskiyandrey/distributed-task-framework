package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.TaskCommandService;

public interface TaskCommandWithDetectorService extends TaskCommandService {

    <T> boolean isOwnTask(TaskDef<T> taskDef);

    boolean isOwnTask(TaskId taskId);
}
