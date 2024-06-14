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

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ExtendedSagaResultRepositoryImpl implements ExtendedSagaResultRepository {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    //language=postgresql
    private static final String SAVE_OR_UPDATE = """
            INSERT INTO _____dtf_saga_result (
                saga_id,
                created_date_utc,
                completed_date_utc,
                is_exception,
                result_type,
                result
            ) VALUES (
                :sagaId::uuid,
                :createdDateUtc,
                :completedDateUtc,
                :isException,
                :resultType,
                :result
            ) ON CONFLICT (saga_id) DO UPDATE
                SET
                    saga_id = excluded.saga_id,
                    created_date_utc = excluded.created_date_utc,
                    completed_date_utc = excluded.completed_date_utc,
                    is_exception = excluded.is_exception,
                    result_type = excluded.result_type,
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

    private MapSqlParameterSource toSqlParameterSource(SagaResultEntity sagaResultEntity) {
        var mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.sagaId, JdbcTools.asNullableString(sagaResultEntity.getSagaId()), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.createdDateUtc, sagaResultEntity.getCreatedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.completedDateUtc, sagaResultEntity.getCompletedDateUtc(), Types.TIMESTAMP);
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.isException, sagaResultEntity.isException(), Types.BOOLEAN);
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.resultType, sagaResultEntity.getResultType(), Types.VARCHAR);
        mapSqlParameterSource.addValue(SagaResultEntity.Fields.result, sagaResultEntity.getResult(), Types.BINARY);
        return mapSqlParameterSource;
    }
}
