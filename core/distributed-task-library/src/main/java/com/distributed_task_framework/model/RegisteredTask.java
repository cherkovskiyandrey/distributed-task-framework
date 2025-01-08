package com.distributed_task_framework.model;

import com.distributed_task_framework.task.Task;
import lombok.Value;
import com.distributed_task_framework.settings.TaskSettings;

@Value(staticConstructor = "of")
public class RegisteredTask<T> {
    Task<T> task;
    TaskSettings taskSettings;
}
