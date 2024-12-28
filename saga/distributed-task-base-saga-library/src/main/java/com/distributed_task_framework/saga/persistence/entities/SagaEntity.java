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
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Table("_____dtf_saga")
@Data
@FieldNameConstants
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class SagaEntity {
    public final static BeanPropertyRowMapper<SagaEntity> SAGA_CONTEXT_ENTITY_MAPPER = new BeanPropertyRowMapper<>(SagaEntity.class);

    @Id
    UUID sagaId;
    String name;
    LocalDateTime createdDateUtc;
    @Nullable
    LocalDateTime completedDateUtc;
    LocalDateTime expirationDateUtc;
    boolean canceled;
    @ToString.Exclude
    byte[] rootTaskId;
    @Nullable
    String exceptionType;
    @Nullable
    @ToString.Exclude
    byte[] result;
    @Nullable
    @ToString.Exclude
    byte[] lastPipelineContext;
}
