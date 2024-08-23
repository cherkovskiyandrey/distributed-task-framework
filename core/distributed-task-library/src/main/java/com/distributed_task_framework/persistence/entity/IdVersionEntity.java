package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.FieldNameConstants;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.util.Comparator;
import java.util.UUID;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@FieldNameConstants
@Builder(toBuilder = true)
public class IdVersionEntity {
    public static Comparator<IdVersionEntity> COMPARATOR = Comparator.comparing(IdVersionEntity::getId);
    public static BeanPropertyRowMapper<IdVersionEntity> ID_VERSION_ROW_MAPPER = new BeanPropertyRowMapper<>(IdVersionEntity.class);

    UUID id;
    Long version;
}
