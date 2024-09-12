package com.distributed_task_framework.saga.persistence.repository.jdbc;

import com.distributed_task_framework.saga.persistence.entities.DlsSagaContextEntity;
import com.distributed_task_framework.saga.persistence.repository.ExtendedDlsSagaContextRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;
import java.time.Clock;
import java.util.Collection;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ExtendedDlsSagaContextRepositoryImpl implements ExtendedDlsSagaContextRepository {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    Clock clock;

    //language=postgresql
    private static final String SAVE_OR_UPDATE = """
            INSERT INTO _____dtf_saga_context_dls (
                saga_id,
                user_name,
                created_date_utc,
                expiration_date_utc,
                root_task_id,
                last_pipeline_context
            ) VALUES (
                :sagaId::uuid,
                :userName,
                :createdDateUtc,
                :expirationDateUtc,
                :rootTaskId,
                :lastPipelineContext
            ) ON CONFLICT (saga_id) DO UPDATE
                SET
                    saga_id = excluded.saga_id,
                    user_name = excluded.user_name,
                    created_date_utc = excluded.created_date_utc,
                    expiration_date_utc = excluded.expiration_date_utc,
                    root_task_id = excluded.root_task_id,
                    last_pipeline_context = excluded.last_pipeline_context
            """;

    @Override
    public Collection<DlsSagaContextEntity> saveOrUpdateAll(Collection<DlsSagaContextEntity> dlsSagaContextEntities) {
        var params = dlsSagaContextEntities.stream()
                .map(this::toSqlParameterSource)
                .toArray(MapSqlParameterSource[]::new);
        int[] affectedRows = namedParameterJdbcTemplate.batchUpdate(
                SAVE_OR_UPDATE,
                params
        );
        return Sets.newHashSet(JdbcTools.filterAffected(Lists.newArrayList(dlsSagaContextEntities), affectedRows));
    }

    private MapSqlParameterSource toSqlParameterSource(DlsSagaContextEntity dlsSagaContextEntity) {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(DlsSagaContextEntity.Fields.sagaId, JdbcTools.asNullableString(dlsSagaContextEntity.getSagaId()), Types.VARCHAR);
        mapSqlParameterSource.addValue(DlsSagaContextEntity.Fields.userName, JdbcTools.asNullableString(dlsSagaContextEntity.getUserName()), Types.VARCHAR);
        mapSqlParameterSource.addValue(DlsSagaContextEntity.Fields.createdDateUtc, dlsSagaContextEntity.getCreatedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(DlsSagaContextEntity.Fields.expirationDateUtc, dlsSagaContextEntity.getExpirationDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(DlsSagaContextEntity.Fields.rootTaskId, dlsSagaContextEntity.getRootTaskId(), Types.BINARY);
        mapSqlParameterSource.addValue(DlsSagaContextEntity.Fields.lastPipelineContext, dlsSagaContextEntity.getLastPipelineContext(), Types.BINARY);
        return mapSqlParameterSource;
    }
}
