package com.distributed_task_framework.perf_test.model;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestState;

import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;

@Value
@Builder
@Jacksonized
public class PerfTestRunResult {
    Long id;
    String name;
    LocalDateTime createdAt;
    @Nullable
    LocalDateTime completedAt;
    @Nullable
    Duration duration;
    String affinityGroup;
    int totalPipelines;
    int totalAffinities;
    int totalTaskOnFirstLevel;
    int totalTaskOnSecondLevel;
    long taskDurationMs;
    Map<PerfTestState, Long> summaryStates;
}
