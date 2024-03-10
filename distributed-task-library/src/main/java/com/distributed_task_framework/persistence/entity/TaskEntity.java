package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.UUID;

@Table("_____dtf_tasks")
@Data
@FieldNameConstants
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class TaskEntity {
    @Id
    UUID id;
    String taskName;
    UUID workflowId;
    @Nullable
    String affinity;
    @Nullable
    String affinityGroup;
    @Version
    Long version; //to provide exactly once guarantee: remove from transaction
    LocalDateTime createdDateUtc;
    UUID assignedWorker;
    LocalDateTime workflowCreatedDateUtc;
    LocalDateTime lastAssignedDateUtc;
    LocalDateTime executionDateUtc;
    boolean singleton;
    boolean notToPlan; //for join tasks
    boolean canceled; //for algorithm of cancellation
    VirtualQueue virtualQueue;
    LocalDateTime deletedAt;
    @Nullable
    @ToString.Exclude
    byte[] messageBytes;
    @Nullable
    @ToString.Exclude
    byte[] joinMessageBytes;
    int failures;
}
