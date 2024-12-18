package com.distributed_task_framework.saga.persistence.repository.jdbc;

import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
import com.distributed_task_framework.saga.persistence.repository.ExtendedSagaContextRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.SqlParameters;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ExtendedSagaContextRepositoryImpl implements ExtendedSagaContextRepository {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    Clock clock;

    //language=postgresql
    private static final String SAVE_OR_UPDATE = """
        INSERT INTO _____dtf_saga (
            saga_id,
            user_name,
            created_date_utc,
            completed_date_utc,
            expiration_date_utc,
            root_task_id,
            exception_type,
            result,
            last_pipeline_context
        ) VALUES (
            :sagaId::uuid,
            :userName,
            :createdDateUtc,
            :completedDateUtc,
            :expirationDateUtc,
            :rootTaskId,
            :exceptionType,
            :result,
            :lastPipelineContext
        ) ON CONFLICT (saga_id) DO UPDATE
            SET
                saga_id = excluded.saga_id,
                user_name = excluded.user_name,
                created_date_utc = excluded.created_date_utc,
                completed_date_utc = excluded.completed_date_utc,
                expiration_date_utc = excluded.expiration_date_utc,
                root_task_id = excluded.root_task_id,
                exception_type = excluded.exception_type,
                result = excluded.result,
                last_pipeline_context = excluded.last_pipeline_context
        """;

    @Override
    public SagaEntity saveOrUpdate(SagaEntity sagaEntity) {
        var parameterSource = toSqlParameterSource(sagaEntity);
        namedParameterJdbcTemplate.update(
            SAVE_OR_UPDATE,
            parameterSource
        );
        return sagaEntity;
    }


    //language=postgresql
    private static final String FIND_EXPIRED = """
        SELECT *
        FROM _____dtf_saga
        WHERE
            completed_date_utc IS NULL
            AND expiration_date_utc <= :expirationDateUtc
        """;

    private final static BeanPropertyRowMapper<SagaEntity> SAGA_CONTEXT_ENTITY_MAPPER = new BeanPropertyRowMapper<>(SagaEntity.class);

    @Override
    public List<SagaEntity> findExpired() {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(SagaEntity.Fields.expirationDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP);
        return namedParameterJdbcTemplate.query(
            FIND_EXPIRED,
            mapSqlParameterSource,
            SAGA_CONTEXT_ENTITY_MAPPER
        );
    }

    //language=postgresql
    private static final String REMOVE_ALL = """
        DELETE FROM _____dtf_saga
        WHERE saga_id = ANY( (:sagaIds)::uuid[] )
        """;

    @Override
    public void removeAll(List<UUID> sagaIds) {
        namedParameterJdbcTemplate.update(
            REMOVE_ALL,
            SqlParameters.of("sagaIds", JdbcTools.UUIDsToStringArray(sagaIds), Types.ARRAY)
        );
    }


    //language=postgresql
    private static final String REMOVE_EXPIRED_RESULT = """
        DELETE FROM _____dtf_saga
        WHERE
            completed_date_utc IS NOT NULL
            AND completed_date_utc < :timeThreshold
        RETURNING saga_id
        """;

    @Override
    public List<UUID> removeCompleted(Duration delay) {
        return namedParameterJdbcTemplate.queryForList(
            REMOVE_EXPIRED_RESULT,
            SqlParameters.of("timeThreshold", LocalDateTime.now(clock).minus(delay), Types.TIMESTAMP),
            UUID.class
        );
    }

    private MapSqlParameterSource toSqlParameterSource(SagaEntity sagaEntity) {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(SagaEntity.Fields.sagaId, JdbcTools.asNullableString(sagaEntity.getSagaId()), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaEntity.Fields.userName, JdbcTools.asNullableString(sagaEntity.getUserName()), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaEntity.Fields.createdDateUtc, sagaEntity.getCreatedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaEntity.Fields.completedDateUtc, sagaEntity.getCompletedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaEntity.Fields.expirationDateUtc, sagaEntity.getExpirationDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaEntity.Fields.rootTaskId, sagaEntity.getRootTaskId(), Types.BINARY);
        mapSqlParameterSource.addValue(SagaEntity.Fields.exceptionType, sagaEntity.getExceptionType(), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaEntity.Fields.result, sagaEntity.getResult(), Types.BINARY);
        mapSqlParameterSource.addValue(SagaEntity.Fields.lastPipelineContext, sagaEntity.getLastPipelineContext(), Types.BINARY);
        return mapSqlParameterSource;
    }
}
