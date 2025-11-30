package com.distributed_task_framework.persistence.entity;

import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.UUID;

@Table("_____dtf_tasks")
@Data
@FieldNameConstants
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class ShortTaskEntity {
    public static Comparator<ShortTaskEntity> COMPARATOR = Comparator.comparing(ShortTaskEntity::getId);

    UUID id;
    String taskName;
    Long version; //to provide exactly once guarantee: remove from transaction
    UUID workflowId;
    @Nullable
    String affinity;
    @Nullable
    String affinityGroup;
    LocalDateTime createdDateUtc;
    UUID assignedWorker;
    LocalDateTime lastAssignedDateUtc;
    LocalDateTime executionDateUtc;
    VirtualQueue virtualQueue;
}
