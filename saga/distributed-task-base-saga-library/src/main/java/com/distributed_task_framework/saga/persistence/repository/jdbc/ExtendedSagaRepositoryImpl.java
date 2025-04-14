package com.distributed_task_framework.saga.persistence.repository.jdbc;

import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
import com.distributed_task_framework.saga.persistence.entities.ShortSagaEntity;
import com.distributed_task_framework.saga.persistence.repository.ExtendedSagaRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.SqlParameters;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ExtendedSagaRepositoryImpl implements ExtendedSagaRepository {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    Clock clock;

    public ExtendedSagaRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcTemplate namedParameterJdbcTemplate,
                                      Clock clock) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.clock = clock;
    }

    //language=postgresql
    private static final String SAVE_OR_UPDATE = """
        INSERT INTO _____dtf_saga (
            saga_id,
            name,
            created_date_utc,
            completed_date_utc,
            available_after_completion_timeout_sec,
            stop_on_failed_any_revert,
            expiration_date_utc,
            root_task_id,
            exception_type,
            result,
            last_pipeline_context
        ) VALUES (
            :sagaId::uuid,
            :name,
            :createdDateUtc,
            :completedDateUtc,
            :availableAfterCompletionTimeoutSec,
            :stopOnFailedAnyRevert,
            :expirationDateUtc,
            :rootTaskId,
            :exceptionType,
            :result,
            :lastPipelineContext
        ) ON CONFLICT (saga_id) DO UPDATE
            SET
                saga_id = excluded.saga_id,
                name = excluded.name,
                created_date_utc = excluded.created_date_utc,
                completed_date_utc = excluded.completed_date_utc,
                available_after_completion_timeout_sec = excluded.available_after_completion_timeout_sec,
                stop_on_failed_any_revert = excluded.stop_on_failed_any_revert,
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
    private static final String FIND_SHORT = """
        SELECT
            saga_id,
            name,
            created_date_utc,
            completed_date_utc,
            expiration_date_utc,
            canceled,
            stop_on_failed_any_revert,
            root_task_id
        FROM _____dtf_saga
        WHERE saga_id = :sagaId::uuid
        """;

    @Override
    public Optional<ShortSagaEntity> findShortById(UUID sagaId) {
        return namedParameterJdbcTemplate.query(
            FIND_SHORT,
            SqlParameters.of(SagaEntity.Fields.sagaId, JdbcTools.asNullableString(sagaId), Types.VARCHAR),
            ShortSagaEntity.SHORT_SAGA_ROW_MAPPER
        ).stream().findAny();
    }


    //language=postgresql
    private static final String IS_COMPLETED = """
        SELECT saga_id, completed_date_utc
        FROM _____dtf_saga
        WHERE saga_id = :sagaId::uuid
        """;

    @Override
    public Optional<Boolean> isCompleted(UUID sagaId) {
        return namedParameterJdbcTemplate.query(
                IS_COMPLETED,
                SqlParameters.of(SagaEntity.Fields.sagaId, JdbcTools.asNullableString(sagaId), Types.VARCHAR),
                ShortSagaEntity.SHORT_SAGA_ROW_MAPPER
            ).stream()
            .map(shortSagaEntity -> shortSagaEntity.getCompletedDateUtc() != null)
            .findAny();
    }


    //language=postgresql
    private static final String IS_CANCELED = """
        SELECT saga_id, canceled
        FROM _____dtf_saga
        WHERE saga_id = :sagaId::uuid
        """;

    @Override
    public Optional<Boolean> isCanceled(UUID sagaId) {
        return namedParameterJdbcTemplate.query(
                IS_CANCELED,
                SqlParameters.of(SagaEntity.Fields.sagaId, JdbcTools.asNullableString(sagaId), Types.VARCHAR),
                ShortSagaEntity.SHORT_SAGA_ROW_MAPPER
            ).stream()
            .map(ShortSagaEntity::isCanceled)
            .findAny();
    }


    //language=postgresql
    private static final String FIND_EXPIRED = """
        SELECT *
        FROM _____dtf_saga
        WHERE
            completed_date_utc IS NULL
            AND expiration_date_utc <= :expirationDateUtc
        """;

    @Override
    public List<SagaEntity> findExpired() {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(SagaEntity.Fields.expirationDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP);
        return namedParameterJdbcTemplate.query(
            FIND_EXPIRED,
            mapSqlParameterSource,
            SagaEntity.SAGA_CONTEXT_ENTITY_MAPPER
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
            AND date_add(completed_date_utc, make_interval(secs => available_after_completion_timeout_sec)) < :timeThreshold
        RETURNING saga_id
        """;

    @Override
    public List<UUID> removeCompleted() {
        return namedParameterJdbcTemplate.queryForList(
            REMOVE_EXPIRED_RESULT,
            SqlParameters.of("timeThreshold", LocalDateTime.now(clock), Types.TIMESTAMP),
            UUID.class
        );
    }

    private MapSqlParameterSource toSqlParameterSource(SagaEntity sagaEntity) {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(SagaEntity.Fields.sagaId, JdbcTools.asNullableString(sagaEntity.getSagaId()), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaEntity.Fields.name, JdbcTools.asNullableString(sagaEntity.getName()), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaEntity.Fields.createdDateUtc, sagaEntity.getCreatedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaEntity.Fields.completedDateUtc, sagaEntity.getCompletedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaEntity.Fields.availableAfterCompletionTimeoutSec, sagaEntity.getAvailableAfterCompletionTimeoutSec(), Types.BIGINT);
        mapSqlParameterSource.addValue(SagaEntity.Fields.stopOnFailedAnyRevert, sagaEntity.isStopOnFailedAnyRevert(), Types.BOOLEAN);
        mapSqlParameterSource.addValue(SagaEntity.Fields.expirationDateUtc, sagaEntity.getExpirationDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaEntity.Fields.rootTaskId, sagaEntity.getRootTaskId(), Types.BINARY);
        mapSqlParameterSource.addValue(SagaEntity.Fields.exceptionType, sagaEntity.getExceptionType(), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaEntity.Fields.result, sagaEntity.getResult(), Types.BINARY);
        mapSqlParameterSource.addValue(SagaEntity.Fields.lastPipelineContext, sagaEntity.getLastPipelineContext(), Types.BINARY);
        return mapSqlParameterSource;
    }
}
