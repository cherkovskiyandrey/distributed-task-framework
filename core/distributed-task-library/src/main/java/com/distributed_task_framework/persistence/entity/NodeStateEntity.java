package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Table("_____dtf_node_state")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class NodeStateEntity {
    public static final BeanPropertyRowMapper<NodeStateEntity> NODE_STATE_ENTITY_BEAN_PROPERTY_ROW_MAPPER =
        new BeanPropertyRowMapper<>(NodeStateEntity.class);
    @Id
    UUID node;
    @Version
    Long version;
    Double medianCpuLoading;
    LocalDateTime lastUpdateDateUtc;
}
