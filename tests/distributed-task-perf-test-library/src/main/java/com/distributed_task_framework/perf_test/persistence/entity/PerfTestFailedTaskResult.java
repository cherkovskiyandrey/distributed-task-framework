package com.distributed_task_framework.perf_test.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import javax.annotation.Nullable;
import java.util.UUID;

@Table("dtf_failed_task_result")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@FieldNameConstants
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class PerfTestFailedTaskResult {
    @Id
    Long id;
    Long runId;
    Long summaryId;
    PerfTestErrorType perfTestErrorType;
    String hierarchy;

    @Nullable
    UUID taskId;
    @Nullable
    Long realNumber;
}
