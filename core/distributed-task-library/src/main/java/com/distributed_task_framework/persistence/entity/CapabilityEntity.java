package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.util.UUID;

@Table("_____dtf_capabilities")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@FieldNameConstants
@Builder(toBuilder = true)
public class CapabilityEntity {
    public static final BeanPropertyRowMapper<CapabilityEntity> CAPABILITY_ENTITY_BEAN_PROPERTY_ROW_MAPPER =
        new BeanPropertyRowMapper<>(CapabilityEntity.class);
    @Id
    @EqualsAndHashCode.Exclude
    UUID id;
    //foreign key to NodeStateEntity#node
    UUID nodeId;
    String value;
}
