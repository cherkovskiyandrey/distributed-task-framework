package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.TaskId;

import java.util.List;

public interface WorkerManager {

    long getCurrentActiveTasks();

    List<TaskId> getCurrentActiveTaskIds();
}
