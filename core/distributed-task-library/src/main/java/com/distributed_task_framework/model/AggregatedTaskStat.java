package com.distributed_task_framework.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import com.distributed_task_framework.persistence.entity.VirtualQueue;

import jakarta.annotation.Nullable;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AggregatedTaskStat {
    @Nullable
    String affinityGroupName;
    String taskName;
    @Nullable
    VirtualQueue virtualQueue;
    boolean notToPlanFlag;
    Integer number;
}
