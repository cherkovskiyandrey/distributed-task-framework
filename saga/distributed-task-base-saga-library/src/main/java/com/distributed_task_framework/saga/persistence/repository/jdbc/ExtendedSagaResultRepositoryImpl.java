package com.distributed_task_framework.saga.persistence.repository.jdbc;

import com.distributed_task_framework.saga.persistence.entities.SagaResultEntity;
import com.distributed_task_framework.saga.persistence.repository.ExtendedSagaResultRepository;
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
public class ExtendedSagaResultRepositoryImpl implements ExtendedSagaResultRepository {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    Clock clock;

    //language=postgresql
    private static final String SAVE_OR_UPDATE = """
            INSERT INTO _____dtf_saga_result (
                saga_id,
                created_date_utc,
                completed_date_utc,
                exception_type,
                result
            ) VALUES (
                :sagaId::uuid,
                :createdDateUtc,
                :completedDateUtc,
                :exceptionType,
                :result
            ) ON CONFLICT (saga_id) DO UPDATE
                SET
                    saga_id = excluded.saga_id,
                    created_date_utc = excluded.created_date_utc,
                    completed_date_utc = excluded.completed_date_utc,
                    exception_type = excluded.exception_type,
                    result = excluded.result
            """;

    @Override
    public SagaResultEntity saveOrUpdate(SagaResultEntity sagaResultEntity) {
        var parameterSource = toSqlParameterSource(sagaResultEntity);
        namedParameterJdbcTemplate.update(
                SAVE_OR_UPDATE,
                parameterSource
        );
        return sagaResultEntity;
    }


    //language=postgresql
    private static final String REMOVE_EXPIRED_EMPTY_RESULT = """
            DELETE FROM _____dtf_saga_result
            WHERE
                completed_date_utc IS NULL
                AND created_date_utc < :timeThreshold
            RETURNING saga_id
            """;

    @Override
    public List<UUID> removeExpiredEmptyResults(Duration delay) {
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
            DELETE FROM _____dtf_saga_result
            WHERE
                completed_date_utc IS NOT NULL
                AND completed_date_utc < :timeThreshold
            RETURNING saga_id
            """;

    @Override
    public List<UUID> removeExpiredResults(Duration delay) {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue("timeThreshold", LocalDateTime.now(clock).minus(delay), Types.TIMESTAMP);
        return namedParameterJdbcTemplate.queryForList(
                REMOVE_EXPIRED_RESULT,
                mapSqlParameterSource,
                UUID.class
        );
    }

    private MapSqlParameterSource toSqlParameterSource(SagaResultEntity sagaResultEntity) {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.sagaId, JdbcTools.asNullableString(sagaResultEntity.getSagaId()), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.createdDateUtc, sagaResultEntity.getCreatedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.completedDateUtc, sagaResultEntity.getCompletedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.exceptionType, sagaResultEntity.getExceptionType(), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.result, sagaResultEntity.getResult(), Types.BINARY);
        return mapSqlParameterSource;
    }
}
