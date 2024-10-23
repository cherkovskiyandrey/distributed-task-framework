package com.distributed_task_framework.task;

import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TestTaskModel<T> {
    TaskDef<T> taskDef;
    Task<T> mockedTask;
    TaskSettings taskSettings;
    @Nullable
    TaskEntity taskEntity;
    @Nullable
    TaskId taskId;
    RegisteredTask<T> registeredTask;
}
