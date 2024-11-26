package com.distributed_task_framework.remote_commands;

import com.distributed_task_framework.model.TaskId;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.time.Duration;

@Value
@Builder
@Jacksonized
public class RescheduleCommand<T> {
    public static final String NAME = "reschedule_command";

    TaskId taskId;
    Duration delay;
}
