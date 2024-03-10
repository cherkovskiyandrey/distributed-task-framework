package com.distributed_task_framework.service.impl.remote_commands;

import com.distributed_task_framework.model.TaskDef;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class RescheduleByTaskDefCommand<T> {
    public static final String NAME = "reschedule_by_task_def_command";

    TaskDef<T> taskDef;
}
