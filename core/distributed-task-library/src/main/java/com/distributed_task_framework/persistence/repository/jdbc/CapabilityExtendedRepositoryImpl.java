package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.persistence.entity.CapabilityEntity;
import com.distributed_task_framework.persistence.repository.CapabilityExtendedRepository;
import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.util.Set;
import java.util.UUID;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class CapabilityExtendedRepositoryImpl implements CapabilityExtendedRepository {
    NamedParameterJdbcOperations jdbcTemplate;

    public CapabilityExtendedRepositoryImpl(DtfJdbcInfrastructure dtfJdbcInfrastructure) {
        this.jdbcTemplate = dtfJdbcInfrastructure.getNamedParameterJdbcOperations();
    }


    private static final String SAVE_OR_UPDATE_BATCH = """
        INSERT INTO _____dtf_capabilities (
            id,
            node_id,
            value
        ) VALUES (
            :id,
            :nodeId,
            :value
        ) ON CONFLICT(id) DO UPDATE
            SET node_id = :nodeId,
            value = :value
        """;

    @Override
    public void saveOrUpdateBatch(Set<CapabilityEntity> currentCapabilities) {
        MapSqlParameterSource[] mapSqlParameterSources = currentCapabilities.stream()
            .map(this::prepareToSave)
            .map(this::toParameterSource)
            .toArray(MapSqlParameterSource[]::new);
        jdbcTemplate.batchUpdate(SAVE_OR_UPDATE_BATCH, mapSqlParameterSources);
    }

    private CapabilityEntity prepareToSave(CapabilityEntity capabilityEntity) {
        if (capabilityEntity.getId() == null) {
            capabilityEntity.setId(UUID.randomUUID());
        }
        return capabilityEntity;
    }

    private MapSqlParameterSource toParameterSource(CapabilityEntity capabilityEntity) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("id", capabilityEntity.getId());
        params.addValue("nodeId", capabilityEntity.getNodeId());
        params.addValue("value", capabilityEntity.getValue());
        return params;
    }
}
