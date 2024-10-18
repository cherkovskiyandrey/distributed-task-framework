package com.distributed_task_framework.service.internal;


import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;

public interface TaskWorker {

    boolean isApplicable(TaskEntity taskEntity, TaskSettings taskParameters);

    <T> void execute(TaskEntity taskEntity, RegisteredTask<T> registeredTask);
}
