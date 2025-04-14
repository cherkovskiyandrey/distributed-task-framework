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
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@FieldNameConstants
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class ShortSagaEntity {
    public static final BeanPropertyRowMapper<ShortSagaEntity> SHORT_SAGA_ROW_MAPPER = new BeanPropertyRowMapper<>(ShortSagaEntity.class);

    UUID sagaId;
    String name;
    LocalDateTime createdDateUtc;
    @Nullable
    LocalDateTime completedDateUtc;
    LocalDateTime expirationDateUtc;
    boolean canceled;
    boolean stopOnFailedAnyRevert;
    @ToString.Exclude
    byte[] rootTaskId;
}
