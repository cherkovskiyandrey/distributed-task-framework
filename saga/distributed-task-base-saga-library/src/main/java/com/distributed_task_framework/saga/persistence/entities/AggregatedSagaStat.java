package com.distributed_task_framework.saga.persistence.entities;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.annotation.Nullable;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class AggregatedSagaStat {
    public final static BeanPropertyRowMapper<AggregatedSagaStat> SAGA_CONTEXT_ENTITY_MAPPER = new BeanPropertyRowMapper<>(AggregatedSagaStat.class);

    public enum State {
        ACTIVE,
        COMPLETED,
        COMPLETED_CLEANING,
        COMPLETED_NOT_CLEANED,
        EXPIRED,
        EXPIRED_NOT_CLEANED,
        UNDEFINED;
    }

    State state;
    @Nullable
    String name;
    Integer number;
}
