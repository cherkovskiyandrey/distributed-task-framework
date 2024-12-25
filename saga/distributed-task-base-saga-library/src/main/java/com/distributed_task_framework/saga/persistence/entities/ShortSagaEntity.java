package com.distributed_task_framework.saga.persistence.entities;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Value
@Builder
public class ShortSagaEntity {
    public static final BeanPropertyRowMapper<ShortSagaEntity> SHORT_SAGA_ROW_MAPPER = new BeanPropertyRowMapper<>(ShortSagaEntity.class);

    UUID sagaId;
    String name;
    LocalDateTime createdDateUtc;
    @Nullable
    LocalDateTime completedDateUtc;
    LocalDateTime expirationDateUtc;
    boolean canceled;
    @ToString.Exclude
    byte[] rootTaskId;
}
