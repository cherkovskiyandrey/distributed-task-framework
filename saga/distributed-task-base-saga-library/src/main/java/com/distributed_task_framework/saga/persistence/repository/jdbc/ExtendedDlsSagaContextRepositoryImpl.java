package com.distributed_task_framework.saga.persistence.repository.jdbc;

import com.distributed_task_framework.saga.persistence.entities.DlsSagaEntity;
import com.distributed_task_framework.saga.persistence.repository.ExtendedDlsSagaContextRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;
import java.util.Collection;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ExtendedDlsSagaContextRepositoryImpl implements ExtendedDlsSagaContextRepository {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public ExtendedDlsSagaContextRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    //language=postgresql
    private static final String SAVE_OR_UPDATE = """
        INSERT INTO _____dtf_saga_dls (
            saga_id,
            name,
            created_date_utc,
            expiration_date_utc,
            root_task_id,
            last_pipeline_context
        ) VALUES (
            :sagaId::uuid,
            :name,
            :createdDateUtc,
            :expirationDateUtc,
            :rootTaskId,
            :lastPipelineContext
        ) ON CONFLICT (saga_id) DO UPDATE
            SET
                saga_id = excluded.saga_id,
                name = excluded.name,
                created_date_utc = excluded.created_date_utc,
                expiration_date_utc = excluded.expiration_date_utc,
                root_task_id = excluded.root_task_id,
                last_pipeline_context = excluded.last_pipeline_context
        """;

    @Override
    public Collection<DlsSagaEntity> saveOrUpdateAll(Collection<DlsSagaEntity> dlsSagaContextEntities) {
        var params = dlsSagaContextEntities.stream()
            .map(this::toSqlParameterSource)
            .toArray(MapSqlParameterSource[]::new);
        int[] affectedRows = namedParameterJdbcTemplate.batchUpdate(
            SAVE_OR_UPDATE,
            params
        );
        return Sets.newHashSet(JdbcTools.filterAffected(Lists.newArrayList(dlsSagaContextEntities), affectedRows));
    }

    private MapSqlParameterSource toSqlParameterSource(DlsSagaEntity dlsSagaEntity) {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(DlsSagaEntity.Fields.sagaId, JdbcTools.asNullableString(dlsSagaEntity.getSagaId()), Types.VARCHAR);
        mapSqlParameterSource.addValue(DlsSagaEntity.Fields.name, JdbcTools.asNullableString(dlsSagaEntity.getName()), Types.VARCHAR);
        mapSqlParameterSource.addValue(DlsSagaEntity.Fields.createdDateUtc, dlsSagaEntity.getCreatedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(DlsSagaEntity.Fields.expirationDateUtc, dlsSagaEntity.getExpirationDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(DlsSagaEntity.Fields.rootTaskId, dlsSagaEntity.getRootTaskId(), Types.BINARY);
        mapSqlParameterSource.addValue(DlsSagaEntity.Fields.lastPipelineContext, dlsSagaEntity.getLastPipelineContext(), Types.BINARY);
        return mapSqlParameterSource;
    }
}
