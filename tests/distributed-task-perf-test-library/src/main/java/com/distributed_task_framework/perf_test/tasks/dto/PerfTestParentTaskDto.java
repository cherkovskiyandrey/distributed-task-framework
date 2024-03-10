package com.distributed_task_framework.perf_test.tasks.dto;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.time.Duration;

@Value
@Builder
@Jacksonized
public class PerfTestParentTaskDto {
    int totalTaskOnFirstLevel;
    int totalTaskOnSecondLevel;
    Duration taskDuration;
}
