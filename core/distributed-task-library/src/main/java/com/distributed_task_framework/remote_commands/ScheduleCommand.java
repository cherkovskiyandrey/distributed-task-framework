package com.distributed_task_framework.remote_commands;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.time.Duration;

@Value
@Builder
@Jacksonized
public class ScheduleCommand<T> {
    public static final String NAME = "schedule_command";

    TaskDef<T> taskDef;
    ExecutionContext<T> executionContext;
    Duration delay;
}
