package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.persistence.entity.IdVersionEntity;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.repository.TaskExtendedRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.SqlParameters;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static java.lang.String.format;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskExtendedRepositoryImpl implements TaskExtendedRepository {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;
    Clock clock;

    public TaskExtendedRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcOperations namedParameterJdbcTemplate,
                                      Clock clock) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.clock = clock;
    }

    private static final String SAVE_OR_UPDATE_TEMPLATE = """
        INSERT INTO _____dtf_tasks (
            id,
            task_name,
            workflow_id,
            affinity,
            affinity_group,
            version,
            created_date_utc,
            assigned_worker,
            workflow_created_date_utc,
            last_assigned_date_utc,
            execution_date_utc,
            singleton,
            not_to_plan,
            canceled,
            virtual_queue,
            deleted_at,
            join_message_bytes,
            message_bytes,
            failures
        ) VALUES (
            :id::uuid,
            :taskName,
            :workflowId::uuid,
            :affinity,
            :affinityGroup,
            :version,
            :createdDateUtc,
            :assignedWorker::uuid,
            :workflowCreatedDateUtc,
            :lastAssignedDateUtc,
            :executionDateUtc,
            :singleton,
            :notToPlan,
            :canceled,
            :virtualQueue::_____dtf_virtual_queue_type,
            :deletedAt,
            :joinMessageBytes,
            :messageBytes,
            :failures
        ) ON CONFLICT (id) DO UPDATE
            SET
                version = excluded.version,
                assigned_worker = excluded.assigned_worker,
                last_assigned_date_utc = excluded.last_assigned_date_utc,
                execution_date_utc = excluded.execution_date_utc,
                singleton = excluded.singleton,
                not_to_plan = excluded.not_to_plan,
                canceled = excluded.canceled,
                virtual_queue = excluded.virtual_queue::_____dtf_virtual_queue_type,
                deleted_at = excluded.deleted_at,
                join_message_bytes = excluded.join_message_bytes,
                message_bytes = excluded.message_bytes,
                failures = excluded.failures
        WHERE _____dtf_tasks.version = :expectedVersion
        """;

    @Override
    public TaskEntity saveOrUpdate(TaskEntity taskEntity) {
        long begin = System.currentTimeMillis();
        taskEntity = prepareToSave(taskEntity);
        var parameterSource = toSqlParameterSource(taskEntity);
        int rowAffected = namedParameterJdbcTemplate.update(SAVE_OR_UPDATE_TEMPLATE, parameterSource);
        if (rowAffected == 0) {
            throw new OptimisticLockException(format("Can't update=[%s]", taskEntity), TaskEntity.class);
        }
        var duration = Duration.ofMillis(System.currentTimeMillis() - begin);
        log.info("saveOrUpdate duration: " + duration);
        return taskEntity;
    }

    @Override
    public Collection<TaskEntity> saveAll(Collection<TaskEntity> taskEntities) {
        List<TaskEntity> preparedTaskEntities = taskEntities.stream().map(this::prepareToSave).toList();
        var sqlParameterSources = preparedTaskEntities.stream()
            .map(this::toSqlParameterSource)
            .toArray(MapSqlParameterSource[]::new);
        int[] updateResult = namedParameterJdbcTemplate.batchUpdate(SAVE_OR_UPDATE_TEMPLATE, sqlParameterSources);
        return IntStream.range(0, updateResult.length)
            .mapToObj(i -> updateResult[i] > 0 ? preparedTaskEntities.get(i) : null)
            .filter(Objects::nonNull).toList();
    }

    private TaskEntity prepareToSave(TaskEntity taskEntity) {
        return taskEntity.toBuilder()
            .id(Optional.ofNullable(taskEntity.getId()).orElse(UUID.randomUUID()))
            .version(Optional.ofNullable(taskEntity.getVersion()).map(v -> v + 1L).orElse(1L))
            .createdDateUtc(taskEntity.getId() == null ? LocalDateTime.now(clock) : taskEntity.getCreatedDateUtc())
            .build();
    }


    private static final String FIND_BY_PRIMARY_KEY = """
        SELECT * FROM _____dtf_tasks
        WHERE id = :id::uuid
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public Optional<TaskEntity> find(UUID taskId) {
        return namedParameterJdbcTemplate.query(
            FIND_BY_PRIMARY_KEY,
            SqlParameters.of(TaskEntity.Fields.id, JdbcTools.asNullableString(taskId), Types.VARCHAR),
            TaskEntity.TASK_ROW_MAPPER
        ).stream().findAny();
    }


    private static final String UPDATE_TASKS_WITH_VERSION = """
        UPDATE _____dtf_tasks
        SET
            version = :version,
            assigned_worker = :assignedWorker::uuid
        WHERE
        (
            _____dtf_tasks.id = :id::uuid
            AND _____dtf_tasks.version = :expectedVersion
        )
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void updateAll(Collection<ShortTaskEntity> plannedTasks) {
        plannedTasks = plannedTasks.stream().map(this::prepareToUpdate).toList();
        var batchArgs = plannedTasks.stream()
            .map(this::toSqlParameterSource)
            .toArray(MapSqlParameterSource[]::new);

        namedParameterJdbcTemplate.batchUpdate(UPDATE_TASKS_WITH_VERSION, batchArgs);
    }

    private ShortTaskEntity prepareToUpdate(ShortTaskEntity plannedTask) {
        return plannedTask.toBuilder()
            .version(plannedTask.getVersion() + 1)
            .build();
    }

    private static final String SELECT_ALL_BY_TASK_NAME = """
        SELECT * FROM _____dtf_tasks
        WHERE
            task_name = :taskName
            AND deleted_at IS NULL
        """;

    //USED INDEXES: _____dtf_tasks_tn_afg_vq_edu_idx
    @Override
    public Collection<TaskEntity> findAllByTaskName(String taskName) {
        return namedParameterJdbcTemplate.query(
            SELECT_ALL_BY_TASK_NAME,
            SqlParameters.of(TaskEntity.Fields.taskName, taskName, Types.VARCHAR),
            TaskEntity.TASK_ROW_MAPPER
        ).stream().toList();
    }


    private static final String SELECT_BY_NAME = """
        SELECT *
        FROM _____dtf_tasks
        WHERE
            (task_name = :taskName)
            AND deleted_at IS NULL
        LIMIT :batchSize;
        """;

    //USED INDEXES: none but very quick because of limit
    @Override
    public Collection<TaskEntity> findByName(String taskName, long batchSize) {
        return namedParameterJdbcTemplate.query(
            SELECT_BY_NAME,
            SqlParameters.of(
                TaskEntity.Fields.taskName, taskName, Types.VARCHAR,
                "batchSize", batchSize, Types.BIGINT
            ),
            TaskEntity.TASK_ROW_MAPPER
        ).stream().toList();
    }

    //language=postgresql
    private static final String FILTER_EXISTED_WORKFLOW_IDS = """
            SELECT workflow_id
            FROM _____dtf_tasks
            WHERE workflow_id = any((:workflowIds)::uuid[])
            AND deleted_at ISNULL
            """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_wid_idx
    @Override
    public Set<UUID> filterExistedWorkflowIds(Set<UUID> workflowIds) {
        return Sets.newHashSet(namedParameterJdbcTemplate.queryForList(
                        FILTER_EXISTED_WORKFLOW_IDS,
                        SqlParameters.of(("workflowIds", JdbcTools.UUIDsToStringArray(workflowIds), Types.VARCHAR),
                        UUID.class
                )
        );
    }

    //language=postgresql
    private static final String FILTER_EXISTED_TASK_IDS = """
            SELECT id
            FROM _____dtf_tasks
            WHERE id = any((:ids)::uuid[])
            AND deleted_at ISNULL
            """;

    //SUPPOSED USED INDEXES: pkey
    @Override
    public Set<UUID> filterExistedTaskIds(Set<UUID> requestedTaskIds) {
        return Sets.newHashSet(namedParameterJdbcTemplate.queryForList(
                        FILTER_EXISTED_TASK_IDS,
                        Map.of("ids", JdbcTools.UUIDsToStringArray(requestedTaskIds)),
                        UUID.class
                )
        );
    }

    //language=postgresql
    private static final String SELECT_BY_IDS = """
        SELECT *
        FROM _____dtf_tasks
        WHERE (id = ANY((:ids)::uuid[]))
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public List<TaskEntity> findAll(Collection<UUID> taskIds) {
        return namedParameterJdbcTemplate.query(
            SELECT_BY_IDS,
            SqlParameters.of("ids", JdbcTools.UUIDsToStringArray(taskIds), Types.ARRAY),
            TaskEntity.TASK_ROW_MAPPER
        ).stream().toList();
    }


    //language=postgresql
    private static final String DELETE_BY_IDS_VERSIONS = """
        DELETE FROM _____dtf_tasks dt
        WHERE (dt.id, dt.version) IN (
            SELECT id, version
            FROM UNNEST(:id::uuid[], :version::int[])
            AS tmp(id, version)
         )
        RETURNING dt.id, dt.version
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public Collection<IdVersionEntity> deleteByIdVersion(Collection<IdVersionEntity> taskIdVersions) {
        var taskIds = taskIdVersions.stream().map(IdVersionEntity::getId).toList();
        var taskVersions = taskIdVersions.stream().map(IdVersionEntity::getVersion).toList();
        return Sets.newHashSet(namedParameterJdbcTemplate.query(
                DELETE_BY_IDS_VERSIONS,
                SqlParameters.of(
                    TaskEntity.Fields.id, JdbcTools.UUIDsToStringArray(taskIds), Types.ARRAY,
                    TaskEntity.Fields.version, JdbcTools.toLongArray(taskVersions), Types.ARRAY
                ),
                IdVersionEntity.ID_VERSION_ROW_MAPPER
            )
        );
    }

    private MapSqlParameterSource toSqlParameterSource(TaskEntity taskEntity) {
        var parameterSource = new MapSqlParameterSource();

        parameterSource.addValue(TaskEntity.Fields.id, JdbcTools.asNullableString(taskEntity.getId()), Types.VARCHAR);
        parameterSource.addValue(TaskEntity.Fields.taskName, taskEntity.getTaskName(), Types.VARCHAR);
        parameterSource.addValue(TaskEntity.Fields.workflowId, JdbcTools.asNullableString(taskEntity.getWorkflowId()), Types.VARCHAR);
        parameterSource.addValue(TaskEntity.Fields.affinity, taskEntity.getAffinity(), Types.VARCHAR);
        parameterSource.addValue(TaskEntity.Fields.affinityGroup, taskEntity.getAffinityGroup(), Types.VARCHAR);
        parameterSource.addValue(TaskEntity.Fields.version, taskEntity.getVersion(), Types.BIGINT);
        parameterSource.addValue(TaskEntity.Fields.createdDateUtc, taskEntity.getCreatedDateUtc(), Types.TIMESTAMP);
        parameterSource.addValue(TaskEntity.Fields.assignedWorker, JdbcTools.asNullableString(taskEntity.getAssignedWorker()), Types.VARCHAR);
        parameterSource.addValue(TaskEntity.Fields.workflowCreatedDateUtc, taskEntity.getWorkflowCreatedDateUtc(), Types.TIMESTAMP);
        parameterSource.addValue(TaskEntity.Fields.lastAssignedDateUtc, taskEntity.getLastAssignedDateUtc(), Types.TIMESTAMP);
        parameterSource.addValue(TaskEntity.Fields.executionDateUtc, taskEntity.getExecutionDateUtc(), Types.TIMESTAMP);
        parameterSource.addValue(TaskEntity.Fields.singleton, taskEntity.isSingleton(), Types.BOOLEAN);
        parameterSource.addValue(TaskEntity.Fields.notToPlan, taskEntity.isNotToPlan(), Types.BOOLEAN);
        parameterSource.addValue(TaskEntity.Fields.canceled, taskEntity.isCanceled(), Types.BOOLEAN);
        parameterSource.addValue(TaskEntity.Fields.virtualQueue, JdbcTools.asString(taskEntity.getVirtualQueue()), Types.VARCHAR);
        parameterSource.addValue(TaskEntity.Fields.deletedAt, taskEntity.getDeletedAt(), Types.TIMESTAMP);
        parameterSource.addValue(TaskEntity.Fields.joinMessageBytes, taskEntity.getJoinMessageBytes(), Types.BINARY);
        parameterSource.addValue(TaskEntity.Fields.messageBytes, taskEntity.getMessageBytes(), Types.BINARY);
        parameterSource.addValue(TaskEntity.Fields.failures, taskEntity.getFailures(), Types.INTEGER);
        parameterSource.addValue("expectedVersion", taskEntity.getVersion() - 1, Types.BIGINT);

        return parameterSource;
    }

    private MapSqlParameterSource toSqlParameterSource(ShortTaskEntity shortTaskEntity) {
        var parameterSource = new MapSqlParameterSource();

        parameterSource.addValue(ShortTaskEntity.Fields.id, JdbcTools.asNullableString(shortTaskEntity.getId()), Types.VARCHAR);
        parameterSource.addValue(ShortTaskEntity.Fields.version, shortTaskEntity.getVersion(), Types.BIGINT);
        parameterSource.addValue(ShortTaskEntity.Fields.assignedWorker, JdbcTools.asNullableString(shortTaskEntity.getAssignedWorker()), Types.VARCHAR);
        parameterSource.addValue("expectedVersion", shortTaskEntity.getVersion() - 1, Types.BIGINT);

        return parameterSource;
    }
}
