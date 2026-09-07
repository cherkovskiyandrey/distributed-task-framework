package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.exception.BatchUpdateException;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.exception.UnknownTaskException;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.TaskIdEntity;
import com.distributed_task_framework.persistence.repository.TaskCommandRepository;
import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.PgComparatorUtils;
import com.distributed_task_framework.utils.SqlParameters;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static java.lang.String.format;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskCommandRepositoryImpl implements TaskCommandRepository {
    public static final String WHERE_CONDITION_PLACEHOLDER = "{WHERE_CONDITION}";
    public static final String SET_STATEMENTS_PLACEHOLDER = "{SET_STATEMENTS}";
    public static final String RETURNING_EXPRESSION = "{RETURNING_EXPRESSION}";
    public static final String NONE_RETURNING = "";

    //language=postgresql
    public static final String ORDERED_UPDATE_OPERATION_TEMPLATE = """
        WITH to_update AS (
            SELECT id FROM _____dtf_tasks
            WHERE
            (
                {WHERE_CONDITION}
            )
            ORDER BY id
            FOR UPDATE
        )
        UPDATE _____dtf_tasks dt
        SET
           {SET_STATEMENTS}
        FROM to_update tu
        WHERE dt.id = tu.id
        {RETURNING_EXPRESSION}
        """;

    NamedParameterJdbcOperations namedParameterJdbcTemplate;
    TaskRepositoryHelper taskRepositoryHelper;
    Clock clock;


    public TaskCommandRepositoryImpl(DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                     TaskRepositoryHelper taskRepositoryHelper,
                                     Clock clock) {
        this.namedParameterJdbcTemplate = dtfJdbcInfrastructure.getNamedParameterJdbcOperations();
        this.taskRepositoryHelper = taskRepositoryHelper;
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
        if (!taskRepositoryHelper.filerExisted(List.of(taskId)).isEmpty()) {
            throw new OptimisticLockException(format("Can't reschedule=[%s]", taskEntity), TaskEntity.class);
        }
        throw new UnknownTaskException(taskId);
    }

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void rescheduleAll(Collection<TaskEntity> taskEntities) {
        taskEntities = PgComparatorUtils.sortTaskEntities(taskEntities);
        var batchArgs = SqlParameters.convert(taskEntities, this::toSqlParameterToReschedule);
        int[] result = namedParameterJdbcTemplate.batchUpdate(RESCHEDULE, batchArgs);
        var notAffected = JdbcTools.filterNotAffected(Lists.newArrayList(taskEntities), result);
        if (notAffected.isEmpty()) {
            return;
        }

        var notAffectedIds = notAffected.stream().map(TaskEntity::getId).toList();
        var optimisticLockIds = taskRepositoryHelper.filerExisted(notAffectedIds);
        var unknownTaskIds = Sets.difference(Sets.newHashSet(notAffectedIds), Sets.newHashSet(optimisticLockIds));

        throw BatchUpdateException.builder()
            .optimisticLockTaskIds(optimisticLockIds)
            .unknownTaskIds(Lists.newArrayList(unknownTaskIds))
            .build();
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
    public void forceRescheduleAll(Collection<TaskEntity> tasksToSave) {
        tasksToSave = PgComparatorUtils.sortTaskEntities(tasksToSave);
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
    private static final String FORCE_RESCHEDULE_ALL_BY_TASK_NAME_WHERE_CONDITION = """
        task_name = :taskName 
        AND deleted_at ISNULL
        """;

    //language=postgresql
    private static final String FORCE_RESCHEDULE_ALL_BY_TASK_NAME_SET_STATEMENT = """
        version = version + 1,
        assigned_worker = null,
        last_assigned_date_utc = null,
        execution_date_utc = execution_date_utc + make_interval(secs => :duration)
        """;

    //language=postgresql
    private static final String FORCE_RESCHEDULE_ALL_BY_TASK_NAME_AND_EXCLUDE_WHERE_CONDITION = """
        task_name = :taskName
        AND NOT (id = ANY( (:ids)::uuid[] ))    
        AND deleted_at ISNULL
        """;

    @Override
    public int forceRescheduleAll(TaskDef<?> taskDef, Duration delay, Collection<TaskId> excludes) {
        if (excludes.isEmpty()) {
            var query = ORDERED_UPDATE_OPERATION_TEMPLATE
                .replace(WHERE_CONDITION_PLACEHOLDER, FORCE_RESCHEDULE_ALL_BY_TASK_NAME_WHERE_CONDITION)
                .replace(SET_STATEMENTS_PLACEHOLDER, FORCE_RESCHEDULE_ALL_BY_TASK_NAME_SET_STATEMENT)
                .replace(RETURNING_EXPRESSION, NONE_RETURNING);
            return namedParameterJdbcTemplate.update(
                query,
                SqlParameters.of(
                    TaskEntity.Fields.taskName, taskDef.getTaskName(), Types.VARCHAR,
                    "duration", delay.toSeconds(), Types.INTEGER
                )
            );
        }

        var query = ORDERED_UPDATE_OPERATION_TEMPLATE
            .replace(WHERE_CONDITION_PLACEHOLDER, FORCE_RESCHEDULE_ALL_BY_TASK_NAME_AND_EXCLUDE_WHERE_CONDITION)
            .replace(SET_STATEMENTS_PLACEHOLDER, FORCE_RESCHEDULE_ALL_BY_TASK_NAME_SET_STATEMENT)
            .replace(RETURNING_EXPRESSION, NONE_RETURNING);
        return namedParameterJdbcTemplate.update(
            query,
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
        taskIds = PgComparatorUtils.sortTaskIds(taskIds);
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
    private static final String CANCEL_ALL_BY_TASK_NAME_WHERE_CONDITIONS = """
        _____dtf_tasks.task_name = :taskName
        AND deleted_at ISNULL
        """;

    //language=postgresql
    private static final String CANCEL_ALL_BY_TASK_NAME_SET_STATEMENTS = """
        version = version + 1,
        canceled = TRUE,
        execution_date_utc = :executionDateUtc
        """;

    //language=postgresql
    private static final String CANCEL_ALL_BY_TASK_NAME_AND_EXCLUDE_WHERE_CONDITION = """
        _____dtf_tasks.task_name = :taskName
        AND NOT (id = ANY( (:ids)::uuid[] ))
        AND deleted_at ISNULL
        """;

    @Override
    public int cancelAll(TaskDef<?> taskDef, Collection<TaskId> excludes) {
        if (excludes.isEmpty()) {
            var query = ORDERED_UPDATE_OPERATION_TEMPLATE
                .replace(WHERE_CONDITION_PLACEHOLDER, CANCEL_ALL_BY_TASK_NAME_WHERE_CONDITIONS)
                .replace(SET_STATEMENTS_PLACEHOLDER, CANCEL_ALL_BY_TASK_NAME_SET_STATEMENTS)
                .replace(RETURNING_EXPRESSION, NONE_RETURNING);
            return namedParameterJdbcTemplate.update(
                query,
                SqlParameters.of(
                    TaskEntity.Fields.taskName, taskDef.getTaskName(), Types.VARCHAR,
                    TaskEntity.Fields.executionDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP
                )
            );
        }

        var query = ORDERED_UPDATE_OPERATION_TEMPLATE
            .replace(WHERE_CONDITION_PLACEHOLDER, CANCEL_ALL_BY_TASK_NAME_AND_EXCLUDE_WHERE_CONDITION)
            .replace(SET_STATEMENTS_PLACEHOLDER, CANCEL_ALL_BY_TASK_NAME_SET_STATEMENTS)
            .replace(RETURNING_EXPRESSION, NONE_RETURNING);
        return namedParameterJdbcTemplate.update(
            query,
            SqlParameters.of(
                TaskEntity.Fields.taskName, taskDef.getTaskName(), Types.VARCHAR,
                TaskEntity.Fields.executionDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP,
                "ids", JdbcTools.UUIDsToStringArray(excludes.stream().map(TaskId::getId).toList()), Types.ARRAY
            )
        );
    }


    //language=postgresql
    private static final String CANCEL_ALL_BY_WORKFLOWS_WHERE_CONDITION = """
        _____dtf_tasks.workflow_id = ANY( (:workflowId)::uuid[] )
        AND deleted_at ISNULL
        """;

    //language=postgresql
    private static final String CANCEL_ALL_BY_WORKFLOWS_SET_STATEMENT = """
        version = version + 1,
        canceled = TRUE,
        execution_date_utc = :executionDateUtc
        """;

    //language=postgresql
    private static final String CANCEL_ALL_BY_WORKFLOWS_RETURNING_FIELDS = """
        RETURNING dt.id, task_name, workflow_id
        """;

    //language=postgresql
    private static final String CANCEL_ALL_BY_WORKFLOWS_AND_EXCLUDE_WHERE_CONDITION = """
        workflow_id = ANY( (:workflowId)::uuid[] )
        AND NOT (id = ANY( (:ids)::uuid[] ))
        AND deleted_at ISNULL
        """;

    @Override
    public Collection<TaskIdEntity> cancelAll(Collection<UUID> workflows, Collection<TaskId> excludes) {
        if (excludes.isEmpty()) {
            var query = ORDERED_UPDATE_OPERATION_TEMPLATE
                .replace(WHERE_CONDITION_PLACEHOLDER, CANCEL_ALL_BY_WORKFLOWS_WHERE_CONDITION)
                .replace(SET_STATEMENTS_PLACEHOLDER, CANCEL_ALL_BY_WORKFLOWS_SET_STATEMENT)
                .replace(RETURNING_EXPRESSION, CANCEL_ALL_BY_WORKFLOWS_RETURNING_FIELDS);
            return namedParameterJdbcTemplate.query(
                query,
                SqlParameters.of(
                    TaskEntity.Fields.workflowId, JdbcTools.UUIDsToStringArray(workflows), Types.ARRAY,
                    TaskEntity.Fields.executionDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP
                ),
                TaskIdEntity.TASK_ID_ROW_MAPPER
            );
        }

        var query = ORDERED_UPDATE_OPERATION_TEMPLATE
            .replace(WHERE_CONDITION_PLACEHOLDER, CANCEL_ALL_BY_WORKFLOWS_AND_EXCLUDE_WHERE_CONDITION)
            .replace(SET_STATEMENTS_PLACEHOLDER, CANCEL_ALL_BY_WORKFLOWS_SET_STATEMENT)
            .replace(RETURNING_EXPRESSION, CANCEL_ALL_BY_WORKFLOWS_RETURNING_FIELDS);
        return namedParameterJdbcTemplate.query(
            query,
            SqlParameters.of(
                TaskEntity.Fields.workflowId, JdbcTools.UUIDsToStringArray(workflows), Types.ARRAY,
                TaskEntity.Fields.executionDateUtc, LocalDateTime.now(clock), Types.TIMESTAMP,
                "ids", JdbcTools.UUIDsToStringArray(excludes.stream().map(TaskId::getId).toList()), Types.ARRAY
            ),
            TaskIdEntity.TASK_ID_ROW_MAPPER
        );
    }
}
