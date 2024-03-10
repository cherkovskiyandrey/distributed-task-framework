package com.distributed_task_framework.perf_test.tasks.dto;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import com.distributed_task_framework.perf_test.model.Hierarchy;

import java.util.UUID;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class PerfTestResultDto {
    Long runId;
    Long summaryId;

    Long intermediateResultId;
    Hierarchy hierarchy;
    UUID taskId;
}
