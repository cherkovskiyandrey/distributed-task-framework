package com.distributed_task_framework.perf_test.tasks.dto;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import com.distributed_task_framework.perf_test.model.Hierarchy;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class PerfTestIntermediateDto {
    Long runId;
    Long summaryId;

    Long parentIntermediateResultId;
    @Builder.Default
    Hierarchy hierarchy = Hierarchy.createEmpty();
}
