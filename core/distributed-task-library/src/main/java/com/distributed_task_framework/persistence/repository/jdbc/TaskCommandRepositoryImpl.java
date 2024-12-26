package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.exception.BatchUpdateException;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.exception.UnknownTaskException;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.TaskIdEntity;
import com.distributed_task_framework.persistence.repository.TaskCommandRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.SqlParameters;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static java.lang.String.format;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskCommandRepositoryImpl implements TaskCommandRepository {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;
    Clock clock;

    public TaskCommandRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcOperations namedParameterJdbcTemplate,
                                     Clock clock) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.clock = clock;
    }


    //language=postgresql
    private static final String RESCHEDULE = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            assigned_worker = null,
            last_assigned_date_utc = null,
            execution_date_utc = :executionDateUtc,
            virtual_queue = :virtualQueue::_____dtf_virtual_queue_type,
            failures = :failures,
            local_state = :localState
        WHERE
        (
            _____dtf_tasks.id = :id::uuid
            AND _____dtf_tasks.version = :version
            AND deleted_at ISNULL
        )
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public TaskEntity reschedule(TaskEntity taskEntity) {
        var args = toSqlParameterToReschedule(taskEntity);
        int rowAffected = namedParameterJdbcTemplate.update(RESCHEDULE, args);
        if (rowAffected == 1) {
            return taskEntity.toBuilder()
                .version(taskEntity.getVersion() + 1)
                .build();
        }

        UUID taskId = taskEntity.getId();
        if (!filerExisted(List.of(taskId)).isEmpty()) {
            throw new OptimisticLockException(format("Can't reschedule=[%s]", taskEntity), TaskEntity.class);
        }
        throw new UnknownTaskException(taskId);
    }


    //todo: integration test
    @Override
    public void rescheduleAll(Collection<TaskEntity> taskEntities) {
        var batchArgs = SqlParameters.convert(taskEntities, this::toSqlParameterToReschedule);
        int[] result = namedParameterJdbcTemplate.batchUpdate(RESCHEDULE, batchArgs);
        var notAffected = JdbcTools.filterNotAffected(Lists.newArrayList(taskEntities), result);
        if (notAffected.isEmpty()) {
            return;
        }

        var notAffectedIds = notAffected.stream().map(TaskEntity::getId).toList();
        var optimisticLockIds = filerExisted(notAffectedIds);
        var unknownTaskIds = Sets.difference(Sets.newHashSet(notAffectedIds), Sets.newHashSet(optimisticLockIds));

        throw BatchUpdateException.builder()
            .optimisticLockTaskIds(optimisticLockIds)
            .unknownTaskIds(Lists.newArrayList(unknownTaskIds))
            .build();
    }


    //language=postgresql
    private static final String FILTER_EXISTED = """
        SELECT id
        FROM _____dtf_tasks
        WHERE
            id = ANY( (:taskIds)::uuid[] )
            AND deleted_at IS NULL
        """;

    private List<UUID> filerExisted(List<UUID> taskIds) {
        return namedParameterJdbcTemplate.queryForList(
            FILTER_EXISTED,
            SqlParameters.of("taskIds", JdbcTools.UUIDsToStringArray(taskIds), Types.ARRAY),
            UUID.class
        );
    }


    //language=postgresql
    private static final String FORCE_RESCHEDULE = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            assigned_worker = null,
            last_assigned_date_utc = null,
            execution_date_utc = :executionDateUtc,
            virtual_queue = :virtualQueue::_____dtf_virtual_queue_type,
            failures = :failures
        WHERE
        (
            _____dtf_tasks.id = :id::uuid
            AND deleted_at ISNULL
        )
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public boolean forceReschedule(TaskEntity taskEntity) {
        var args = toSqlParameterToReschedule(taskEntity);
        return namedParameterJdbcTemplate.update(FORCE_RESCHEDULE, args) == 1;
    }

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void forceRescheduleAll(List<TaskEntity> tasksToSave) {
        var batchArgs = SqlParameters.convert(tasksToSave, this::toSqlParameterToReschedule);
        namedParameterJdbcTemplate.batchUpdate(FORCE_RESCHEDULE, batchArgs);
    }

    private SqlParameterSource toSqlParameterToReschedule(TaskEntity taskEntity) {
        return SqlParameters.of(
            TaskEntity.Fields.id, taskEntity.getId(), Types.VARCHAR,
            TaskEntity.Fields.executionDateUtc, taskEntity.getExecutionDateUtc(), Types.TIMESTAMP,
            TaskEntity.Fields.virtualQueue, JdbcTools.asString(taskEntity.getVirtualQueue()), Types.VARCHAR,
            TaskEntity.Fields.failures, taskEntity.getFailures(), Types.INTEGER,
            TaskEntity.Fields.version, taskEntity.getVersion(), Types.INTEGER,
            TaskEntity.Fields.localState, taskEntity.getLocalState(), Types.BINARY
        );
    }


    //language=postgresql
    private static final String FORCE_RESCHEDULE_ALL_BY_TASK_NAME = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            assigned_worker = null,
            last_assigned_date_utc = null,
            execution_date_utc = execution_date_utc + make_interval(secs => :duration)
        WHERE
        (
            _____dtf_tasks.task_name = :taskName
            AND deleted_at ISNULL
        )
        """;

    //language=postgresql
    private static final String FORCE_RESCHEDULE_ALL_BY_TASK_NAME_AND_EXCLUDE = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            assigned_worker = null,
            last_assigned_date_utc = null,
            execution_date_utc = execution_date_utc + make_interval(secs => :duration)
        WHERE
        (
            _____dtf_tasks.task_name = :taskName
            AND NOT (id = ANY( (:ids)::uuid[] ))
            AND deleted_at ISNULL
        )
        """;


    @Override
    public int forceRescheduleAll(TaskDef<?> taskDef, Duration delay, Collection<TaskId> excludes) {
        if (excludes.isEmpty()) {
            return namedParameterJdbcTemplate.update(
                FORCE_RESCHEDULE_ALL_BY_TASK_NAME,
                SqlParameters.of(
                    TaskEntity.Fields.taskName, taskDef.getTaskName(), Types.VARCHAR,
                    "duration", delay.toSeconds(), Types.INTEGER
                )
            );
        }

        return namedParameterJdbcTemplate.update(
            FORCE_RESCHEDULE_ALL_BY_TASK_NAME_AND_EXCLUDE,
            SqlParameters.of(
                TaskEntity.Fields.taskName, taskDef.getTaskName(), Types.VARCHAR,
                "duration", delay.toSeconds(), Types.INTEGER,
                "ids", JdbcTools.UUIDsToStringArray(excludes.stream().map(TaskId::getId).toList()), Types.ARRAY
            )
        );
    }


    //language=postgresql
    private static final String CANCEL_TASK_BY_TASK_ID = """
        UPDATE _____dtf_tasks
        SET
            version = version + 1,
            canceled = TRUE,
            execution_date_utc = :executionDateUtc
        WHERE
        (
            _____dtf_tasks.id = :id::uuid
            AND deleted_at ISNULL
        )
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public boolean cancel(UUID taskId) {
        int updatedRows = namedParameterJdbcTemplate.update(
            CANCEL_TASK_BY_TASK_ID,
            toSqlParameterToCancel(taskId)
        );
        return updatedRows == 1;
    }

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void cancelAll(Collection<UUID> taskIds) {
        var batchArgs = SqlParameters.convert(taskIds, this::toSqlParameterToCancel);
        namedParameterJdbcTemplate.batchUpdate(CANCEL_TASK_BY_TASK_ID, batchArgs);
    }

    private SqlParameterSource toSqlParameterToCancel(UUID taskId) {
        return SqlParameters.of(
            TaskEntity.Fields.id, JdbcTools.asNullableString(taskId), Types.VARCHAR,
            //in order to not wait for tasks scheduled to future.
            TaskEntity.Fields.executionDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP
        );
    }


    //language=postgresql
    private static final String CANCEL_ALL_BY_TASK_NAME = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            canceled = TRUE,
            execution_date_utc = :executionDateUtc
        WHERE
        (
            _____dtf_tasks.task_name = :taskName
            AND deleted_at ISNULL
        )
        """;

    //language=postgresql
    private static final String CANCEL_ALL_BY_TASK_NAME_AND_EXCLUDE = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            canceled = TRUE,
            execution_date_utc = :executionDateUtc
        WHERE
        (
            _____dtf_tasks.task_name = :taskName
            AND NOT (id = ANY( (:ids)::uuid[] ))
            AND deleted_at ISNULL
        )
        """;

    @Override
    public int cancelAll(TaskDef<?> taskDef, Collection<TaskId> excludes) {
        if (excludes.isEmpty()) {
            return namedParameterJdbcTemplate.update(
                CANCEL_ALL_BY_TASK_NAME,
                SqlParameters.of(
                    TaskEntity.Fields.taskName, taskDef.getTaskName(), Types.VARCHAR,
                    TaskEntity.Fields.executionDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP
                )
            );
        }

        return namedParameterJdbcTemplate.update(
            CANCEL_ALL_BY_TASK_NAME_AND_EXCLUDE,
            SqlParameters.of(
                TaskEntity.Fields.taskName, taskDef.getTaskName(), Types.VARCHAR,
                TaskEntity.Fields.executionDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP,
                "ids", JdbcTools.UUIDsToStringArray(excludes.stream().map(TaskId::getId).toList()), Types.ARRAY
            )
        );
    }


    //language=postgresql
    private static final String CANCEL_ALL_BY_WORKFLOWS = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            canceled = TRUE,
            execution_date_utc = :executionDateUtc
        WHERE
        (
            _____dtf_tasks.workflow_id = ANY( (:workflowId)::uuid[] )
            AND deleted_at ISNULL
        )
        RETURNING id, task_name, workflow_id
        """;

    //language=postgresql
    private static final String CANCEL_ALL_BY_WORKFLOWS_AND_EXCLUDE = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            canceled = TRUE,
            execution_date_utc = :executionDateUtc
        WHERE
        (
            _____dtf_tasks.workflow_id = ANY( (:workflowId)::uuid[] )
            AND NOT (id = ANY( (:ids)::uuid[] ))
            AND deleted_at ISNULL
        )
        RETURNING id, task_name, workflow_id
        """;

    @Override
    public Collection<TaskIdEntity> cancelAll(Collection<UUID> workflows, Collection<TaskId> excludes) {
        if (excludes.isEmpty()) {
            return namedParameterJdbcTemplate.query(
                CANCEL_ALL_BY_WORKFLOWS,
                SqlParameters.of(
                    TaskEntity.Fields.workflowId, JdbcTools.UUIDsToStringArray(workflows), Types.ARRAY,
                    TaskEntity.Fields.executionDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP
                ),
                TaskIdEntity.TASK_ID_ROW_MAPPER
            );
        }

        return namedParameterJdbcTemplate.query(
            CANCEL_ALL_BY_WORKFLOWS_AND_EXCLUDE,
            SqlParameters.of(
                TaskEntity.Fields.workflowId, JdbcTools.UUIDsToStringArray(workflows), Types.ARRAY,
                TaskEntity.Fields.executionDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP,
                "ids", JdbcTools.UUIDsToStringArray(excludes.stream().map(TaskId::getId).toList()), Types.ARRAY
            ),
            TaskIdEntity.TASK_ID_ROW_MAPPER
        );
    }
}
