package com.distributed_task_framework.remote_commands;

import com.distributed_task_framework.model.TaskDef;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class CancelTaskByTaskDefCommand<T> {
    public static final String NAME = "cancel_by_task_id_command";

    TaskDef<T> taskDef;
}
