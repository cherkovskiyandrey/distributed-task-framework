package com.distributed_task_framework.perf_test.tasks.dto;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;

@Valid
@Value
@Builder
@Jacksonized
public class PerfTestGeneratedSpecDto {
    @NotEmpty
    String name;
    @NotEmpty
    String affinityGroup;
    @Min(1)
    int totalPipelines;
    @Min(1)
    int totalAffinities;
    @Min(1)
    int totalTaskOnFirstLevel;
    @Min(1)
    int totalTaskOnSecondLevel;
    @Min(1)
    long taskDurationMs;
}
