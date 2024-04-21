package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.exception.BatchUpdateException;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.exception.UnknownTaskException;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.repository.TaskCommandRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class TaskCommandRepositoryImpl implements TaskCommandRepository {
    JdbcTemplate jdbcTemplate;
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private static final String RESCHEDULE = """
            UPDATE _____dtf_tasks
            SET version = version + 1,
                assigned_worker = ?,
                last_assigned_date_utc = ?,
                execution_date_utc = ?,
                virtual_queue = ?::_____dtf_virtual_queue_type
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
        int rowAffected = jdbcTemplate.update(RESCHEDULE, args);
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
        int[] result = jdbcTemplate.batchUpdate(RESCHEDULE, batchArgs);
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
                Map.of("taskIds", JdbcTools.UUIDsToStringArray(taskIds)),
                UUID.class
        );
    }


    private static final String RESCHEDULE_IGNORE_VERSION = """
            UPDATE _____dtf_tasks
            SET version = version + 1,
                assigned_worker = ?,
                last_assigned_date_utc = ?,
                execution_date_utc = ?,
                failures = ?,
                virtual_queue = ?::_____dtf_virtual_queue_type
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
        return jdbcTemplate.update(RESCHEDULE_IGNORE_VERSION, args) == 1;
    }

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void rescheduleAllIgnoreVersion(List<TaskEntity> tasksToSave) {
        List<Object[]> batchArgs = tasksToSave.stream()
                .map(this::toArrayOfParamsIgnoreVersionToRescheduleAll)
                .toList();
        jdbcTemplate.batchUpdate(RESCHEDULE_IGNORE_VERSION, batchArgs);
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
        int updatedRows = jdbcTemplate.update(CANCEL_TASK_BY_TASK_ID, taskId);
        return updatedRows == 1;
    }

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void cancelAll(Collection<UUID> taskIds) {
        List<Object[]> batchArgs = taskIds.stream()
                .map(this::toArrayOfParamsToCancel)
                .toList();

        jdbcTemplate.batchUpdate(CANCEL_TASK_BY_TASK_ID, batchArgs);
    }

    private Object[] toArrayOfParamsToReschedule(TaskEntity taskEntity) {
        Object[] result = new Object[6];
        result[0] = taskEntity.getAssignedWorker();
        result[1] = taskEntity.getLastAssignedDateUtc();
        result[2] = taskEntity.getExecutionDateUtc();
        result[3] = JdbcTools.asString(taskEntity.getVirtualQueue());
        result[4] = taskEntity.getId();
        result[5] = taskEntity.getVersion();
        return result;
    }

    private Object[] toArrayOfParamsIgnoreVersionToRescheduleAll(TaskEntity taskEntity) {
        Object[] result = new Object[6];
        result[0] = taskEntity.getAssignedWorker();
        result[1] = taskEntity.getLastAssignedDateUtc();
        result[2] = taskEntity.getExecutionDateUtc();
        result[3] = taskEntity.getFailures();
        result[4] = JdbcTools.asString(taskEntity.getVirtualQueue());
        result[5] = taskEntity.getId();
        return result;
    }

    private Object[] toArrayOfParamsToCancel(UUID taskId) {
        Object[] result = new Object[1];
        result[0] = taskId;
        return result;
    }
}
