package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.settings.TaskSettings;
import lombok.Value;

@Value(staticConstructor = "of")
public class RemoteTaskWithParameters<T> {
    TaskDef<T> taskDef;
    TaskSettings taskSettings;
}
