package com.distributed_task_framework.service.impl.remote_commands;

import com.distributed_task_framework.model.TaskId;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class CancelTaskCommand {
    public static final String NAME = "cancel_command";

    TaskId taskId;
}
