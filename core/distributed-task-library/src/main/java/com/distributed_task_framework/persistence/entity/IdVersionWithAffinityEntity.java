package com.distributed_task_framework.persistence.entity;

import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.FieldNameConstants;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.util.UUID;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@FieldNameConstants
@Builder(toBuilder = true)
public class IdVersionWithAffinityEntity {
    public static final BeanPropertyRowMapper<IdVersionWithAffinityEntity> ID_VERSION_WITH_AFFINITY_ROW_MAPPER = new BeanPropertyRowMapper<>(IdVersionWithAffinityEntity.class);
    UUID id;
    Long version;
    @Nullable
    String affinityGroup;
    @Nullable
    String affinity;
}
