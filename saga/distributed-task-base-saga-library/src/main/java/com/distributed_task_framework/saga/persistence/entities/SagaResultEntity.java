package com.distributed_task_framework.saga.persistence.entities;

import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@Table("_____dtf_saga_result")
@Value
@FieldNameConstants
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
public class SagaResultEntity {
    @Id
    UUID sagaId;
    LocalDateTime createdDateUtc;
    LocalDateTime completedDateUtc;
    @Nullable
    String exceptionType;
    @Nullable
    @ToString.Exclude
    byte[] result;
}
