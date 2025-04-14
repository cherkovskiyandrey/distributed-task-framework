package com.distributed_task_framework.saga.persistence.entities;

import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@Table("_____dtf_saga_dls")
@Data
@FieldNameConstants
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class DlsSagaEntity {
    @Id
    UUID sagaId;
    String name;
    LocalDateTime createdDateUtc;
    long availableAfterCompletionTimeoutSec;
    boolean stopOnFailedAnyRevert;
    LocalDateTime expirationDateUtc;
    @ToString.Exclude
    byte[] rootTaskId;
    @Nullable
    @ToString.Exclude
    byte[] lastPipelineContext;
}
