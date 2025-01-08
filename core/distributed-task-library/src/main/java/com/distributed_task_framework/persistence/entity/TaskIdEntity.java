package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.util.UUID;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class TaskIdEntity {
    public static final BeanPropertyRowMapper<TaskIdEntity> TASK_ID_ROW_MAPPER = new BeanPropertyRowMapper<>(TaskIdEntity.class);

    String taskName;
    UUID id;
    UUID workflowId;
}
