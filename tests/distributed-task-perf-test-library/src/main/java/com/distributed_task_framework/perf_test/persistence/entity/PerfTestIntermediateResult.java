package com.distributed_task_framework.perf_test.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

/**
 * The idea here to have common entity for result.
 * Entity is identified by (affinityGroup + affinity + hierarchy).
 * DTF provide guarantees not to run parallel tasks with same affinityGroup + affinity + workflowId.
 * It means that this entity is a good candidate to check this guaranty.
 */
@Table("dtf_perf_test_intermediate_result")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@FieldNameConstants
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class PerfTestIntermediateResult {
    @Id
    @EqualsAndHashCode.Exclude
    Long id;
    String affinityGroup;
    String affinity;
    String hierarchy;
    Long number;
}
