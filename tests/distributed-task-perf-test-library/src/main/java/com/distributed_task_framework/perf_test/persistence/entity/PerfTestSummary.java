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

import jakarta.annotation.Nullable;
import java.time.LocalDateTime;

@Table("dtf_perf_test_summary")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@FieldNameConstants
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class PerfTestSummary {
    @Id
    Long id;
    Long testRunId;
    @Nullable
    LocalDateTime completedAt;
    Long testId;
    String affinity;
    Long number;
    PerfTestState state;
}
