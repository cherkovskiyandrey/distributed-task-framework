package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@Table("_____dtf_dlt_tasks")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class DltEntity {
    @Id
    UUID id;
    String taskName;
    @Version
    Long version;
    UUID workflowId;
    LocalDateTime createdDateUtc;
    LocalDateTime lastAssignedDateUtc;
    LocalDateTime executionDateUtc;
    @ToString.Exclude
    byte[] messageBytes;
    int failures;
}
