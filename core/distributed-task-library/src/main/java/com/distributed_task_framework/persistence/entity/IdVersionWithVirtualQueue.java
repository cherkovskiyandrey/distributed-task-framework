package com.distributed_task_framework.persistence.entity;

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
public class IdVersionWithVirtualQueue {
    public static BeanPropertyRowMapper<IdVersionWithVirtualQueue> ID_VERSION_WITH_VIRTUAL_QUEUE_ROW_MAPPER =
        new BeanPropertyRowMapper<>(IdVersionWithVirtualQueue.class);

    UUID id;
    Long version;
    VirtualQueue virtualQueue;
}
