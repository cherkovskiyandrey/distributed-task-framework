package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.persistence.entity.RegisteredTaskEntity;
import com.distributed_task_framework.persistence.repository.RegisteredTaskExtendedRepository;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.util.Collection;
import java.util.UUID;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RegisteredTaskExtendedRepositoryImpl implements RegisteredTaskExtendedRepository {
    NamedParameterJdbcOperations jdbcTemplate;

    public RegisteredTaskExtendedRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcOperations jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static final String SAVE_OR_UPDATE_BATCH = """
        INSERT INTO _____dtf_registered_tasks (
            id,
            node_state_id,
            task_name
        ) VALUES (
            :id,
            :nodeStateId,
            :taskName
        ) ON CONFLICT(id) DO UPDATE
            SET node_state_id = :nodeStateId,
            task_name = :taskName
        """;

    @Override
    public void saveOrUpdateBatch(Collection<RegisteredTaskEntity> taskEntities) {
        MapSqlParameterSource[] mapSqlParameterSources = taskEntities.stream()
            .map(this::prepareToSave)
            .map(this::toParameterSource)
            .toArray(MapSqlParameterSource[]::new);
        jdbcTemplate.batchUpdate(SAVE_OR_UPDATE_BATCH, mapSqlParameterSources);
    }

    private RegisteredTaskEntity prepareToSave(RegisteredTaskEntity registeredTaskEntity) {
        if (registeredTaskEntity.getId() == null) {
            registeredTaskEntity.setId(UUID.randomUUID());
        }
        return registeredTaskEntity;
    }

    private MapSqlParameterSource toParameterSource(RegisteredTaskEntity registeredTaskEntity) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("id", registeredTaskEntity.getId());
        params.addValue("nodeStateId", registeredTaskEntity.getNodeStateId());
        params.addValue("taskName", registeredTaskEntity.getTaskName());
        return params;
    }
}
