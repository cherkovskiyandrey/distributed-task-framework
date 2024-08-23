package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.exception.BatchUpdateException;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.exception.UnknownTaskException;
import com.distributed_task_framework.persistence.entity.TaskEntity;
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

import java.lang.reflect.Type;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static java.lang.String.format;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskCommandRepositoryImpl implements TaskCommandRepository {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;

    public TaskCommandRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcOperations namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    private static final String RESCHEDULE = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            assigned_worker = ?,
            last_assigned_date_utc = ?,
            execution_date_utc = ?,
            virtual_queue = ?::_____dtf_virtual_queue_type,
            failures = ?
        WHERE
        (
            _____dtf_tasks.id = ?
            AND _____dtf_tasks.version = ?
            AND deleted_at ISNULL
        )
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void reschedule(TaskEntity taskEntity) {
        Object[] args = toArrayOfParamsToReschedule(taskEntity);
        int rowAffected = namedParameterJdbcTemplate.getJdbcOperations()
            .update(RESCHEDULE, args);
        if (rowAffected == 1) {
            return;
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
        var batchArgs = taskEntities.stream()
            .map(this::toArrayOfParamsToReschedule)
            .toList();
        int[] result = namedParameterJdbcTemplate.getJdbcOperations()
            .batchUpdate(RESCHEDULE, batchArgs);
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

    private static final String FILTER_EXISTED = """
        SELECT id
        FROM _____dtf_tasks
        WHERE
            id = ANY( (:taskIds)::uuid[] )
            AND virtual_queue <> 'DELETED'::_____dtf_virtual_queue_type
        """;

    private List<UUID> filerExisted(List<UUID> taskIds) {
        return namedParameterJdbcTemplate.queryForList(
            FILTER_EXISTED,
            SqlParameters.of("taskIds", JdbcTools.UUIDsToStringArray(taskIds), Types.ARRAY),
            UUID.class
        );
    }


    private static final String RESCHEDULE_IGNORE_VERSION = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            assigned_worker = ?,
            last_assigned_date_utc = ?,
            execution_date_utc = ?,
            virtual_queue = ?::_____dtf_virtual_queue_type,
            failures = ?
        WHERE
        (
            _____dtf_tasks.id = ?
            AND deleted_at ISNULL
        )
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public boolean forceReschedule(TaskEntity taskEntity) {
        Object[] args = toArrayOfParamsIgnoreVersionToRescheduleAll(taskEntity);
        return namedParameterJdbcTemplate.getJdbcOperations().update(RESCHEDULE_IGNORE_VERSION, args) == 1;
    }

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void rescheduleAllIgnoreVersion(List<TaskEntity> tasksToSave) {
        List<Object[]> batchArgs = tasksToSave.stream()
            .map(this::toArrayOfParamsIgnoreVersionToRescheduleAll)
            .toList();
        namedParameterJdbcTemplate.getJdbcOperations().batchUpdate(RESCHEDULE_IGNORE_VERSION, batchArgs);
    }

    private static final String CANCEL_TASK_BY_TASK_ID = """
        UPDATE _____dtf_tasks
        SET version = version + 1,
            canceled = TRUE
        WHERE
        (
            _____dtf_tasks.id = ?
            AND deleted_at ISNULL
        )
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public boolean cancel(UUID taskId) {
        int updatedRows = namedParameterJdbcTemplate.getJdbcOperations().update(CANCEL_TASK_BY_TASK_ID, taskId);
        return updatedRows == 1;
    }

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void cancelAll(Collection<UUID> taskIds) {
        List<Object[]> batchArgs = taskIds.stream()
            .map(this::toArrayOfParamsToCancel)
            .toList();

        namedParameterJdbcTemplate.getJdbcOperations().batchUpdate(CANCEL_TASK_BY_TASK_ID, batchArgs);
    }

    private Object[] toArrayOfParamsToReschedule(TaskEntity taskEntity) {
        return toParamArray(
            taskEntity.getAssignedWorker(),
            taskEntity.getLastAssignedDateUtc(),
            taskEntity.getExecutionDateUtc(),
            JdbcTools.asString(taskEntity.getVirtualQueue()),
            taskEntity.getFailures(),
            taskEntity.getId(),
            taskEntity.getVersion()
        );
    }

    private Object[] toArrayOfParamsIgnoreVersionToRescheduleAll(TaskEntity taskEntity) {
        return toParamArray(
            taskEntity.getAssignedWorker(),
            taskEntity.getLastAssignedDateUtc(),
            taskEntity.getExecutionDateUtc(),
            JdbcTools.asString(taskEntity.getVirtualQueue()),
            taskEntity.getFailures(),
            taskEntity.getId()
        );
    }

    private Object[] toArrayOfParamsToCancel(UUID taskId) {
        return toParamArray(taskId);
    }

    private static Object[] toParamArray(Object... values) {
        return values;
    }
}
