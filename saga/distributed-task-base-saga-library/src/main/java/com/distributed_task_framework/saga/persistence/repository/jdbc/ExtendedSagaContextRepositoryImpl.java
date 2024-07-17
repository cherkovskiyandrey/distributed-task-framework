package com.distributed_task_framework.saga.persistence.repository.jdbc;

import com.distributed_task_framework.saga.persistence.entities.SagaContextEntity;
import com.distributed_task_framework.saga.persistence.repository.ExtendedSagaContextRepository;
import com.distributed_task_framework.utils.JdbcTools;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
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
            INSERT INTO _____dtf_saga_context (
                saga_id,
                created_date_utc,
                completed_date_utc,
                root_task_id,
                exception_type,
                result
            ) VALUES (
                :sagaId::uuid,
                :createdDateUtc,
                :completedDateUtc,
                :rootTaskId,
                :exceptionType,
                :result
            ) ON CONFLICT (saga_id) DO UPDATE
                SET
                    saga_id = excluded.saga_id,
                    created_date_utc = excluded.created_date_utc,
                    completed_date_utc = excluded.completed_date_utc,
                    root_task_id = excluded.root_task_id,
                    exception_type = excluded.exception_type,
                    result = excluded.result
            """;

    @Override
    public SagaContextEntity saveOrUpdate(SagaContextEntity sagaContextEntity) {
        var parameterSource = toSqlParameterSource(sagaContextEntity);
        namedParameterJdbcTemplate.update(
                SAVE_OR_UPDATE,
                parameterSource
        );
        return sagaContextEntity;
    }


    //language=postgresql
    private static final String REMOVE_EXPIRED_EMPTY_RESULT = """
            DELETE FROM _____dtf_saga_context
            WHERE
                completed_date_utc IS NULL
                AND created_date_utc < :timeThreshold
            RETURNING saga_id
            """;

    @Override
    public List<UUID> removeHanging(Duration delay) {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue("timeThreshold", LocalDateTime.now(clock).minus(delay), Types.TIMESTAMP);
        return namedParameterJdbcTemplate.queryForList(
                REMOVE_EXPIRED_EMPTY_RESULT,
                mapSqlParameterSource,
                UUID.class
        );
    }


    //language=postgresql
    private static final String REMOVE_EXPIRED_RESULT = """
            DELETE FROM _____dtf_saga_context
            WHERE
                completed_date_utc IS NOT NULL
                AND completed_date_utc < :timeThreshold
            RETURNING saga_id
            """;

    @Override
    public List<UUID> removeExpired(Duration delay) {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue("timeThreshold", LocalDateTime.now(clock).minus(delay), Types.TIMESTAMP);
        return namedParameterJdbcTemplate.queryForList(
                REMOVE_EXPIRED_RESULT,
                mapSqlParameterSource,
                UUID.class
        );
    }

    private MapSqlParameterSource toSqlParameterSource(SagaContextEntity sagaContextEntity) {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(SagaContextEntity.Fields.sagaId, JdbcTools.asNullableString(sagaContextEntity.getSagaId()), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaContextEntity.Fields.createdDateUtc, sagaContextEntity.getCreatedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaContextEntity.Fields.completedDateUtc, sagaContextEntity.getCompletedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaContextEntity.Fields.rootTaskId, sagaContextEntity.getRootTaskId(), Types.BINARY);
        mapSqlParameterSource.addValue(SagaContextEntity.Fields.exceptionType, sagaContextEntity.getExceptionType(), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaContextEntity.Fields.result, sagaContextEntity.getResult(), Types.BINARY);
        return mapSqlParameterSource;
    }
}
