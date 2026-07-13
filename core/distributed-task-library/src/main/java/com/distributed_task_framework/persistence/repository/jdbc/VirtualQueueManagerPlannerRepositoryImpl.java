package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.exception.BatchUpdateException;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.exception.UnknownTaskException;
import com.distributed_task_framework.model.AffinityGroupAndAffinity;
import com.distributed_task_framework.model.AffinityGroupStat;
import com.distributed_task_framework.model.AffinityGroupWrapper;
import com.distributed_task_framework.persistence.entity.IdVersionEntity;
import com.distributed_task_framework.persistence.entity.IdVersionWithAffinityEntity;
import com.distributed_task_framework.persistence.entity.IdVersionWithVirtualQueue;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.VirtualQueueManagerPlannerRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.SqlParameters;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.distributed_task_framework.persistence.entity.IdVersionWithVirtualQueue.ID_VERSION_WITH_VIRTUAL_QUEUE_ROW_MAPPER;
import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static java.lang.String.format;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class VirtualQueueManagerPlannerRepositoryImpl implements VirtualQueueManagerPlannerRepository {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;
    TaskRepositoryHelper taskRepositoryHelper;
    Clock clock;

    public VirtualQueueManagerPlannerRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcOperations namedParameterJdbcTemplate,
                                                    TaskRepositoryHelper taskRepositoryHelper,
                                                    Clock clock) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.taskRepositoryHelper = taskRepositoryHelper;
        this.clock = clock;
    }

    //language=postgresql
    private static final String SELECT_MAX_CREATED_DATE_IN_NEW_VIRTUAL_QUEUE = """
        SELECT max(created_date_utc)
        FROM _____dtf_tasks
        WHERE virtual_queue = 'NEW'::_____dtf_virtual_queue_type
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_vq_cdu_idx
    @Override
    public Optional<LocalDateTime> maxCreatedDateInNewVirtualQueue() {
        return Optional.ofNullable(namedParameterJdbcTemplate.queryForObject(
                SELECT_MAX_CREATED_DATE_IN_NEW_VIRTUAL_QUEUE,
                Collections.emptyMap(),
                LocalDateTime.class
            )
        );
    }

    //language=postgresql
    private static final String SELECT_AFFINITY_GROUPS_IN_VIRTUAL_QUEUE = """
        SELECT DISTINCT affinity_group
        FROM _____dtf_tasks
        WHERE
        	virtual_queue = 'NEW'::_____dtf_virtual_queue_type
        	AND created_date_utc >= :from - make_interval(secs => :timeOverlapSec)
        """;

    private static final BeanPropertyRowMapper<AffinityGroupWrapper> AFFINITY_GROUP_WRAPPER_BEAN_PROPERTY_ROW_MAPPER =
        new BeanPropertyRowMapper<>(AffinityGroupWrapper.class);

    //SUPPOSED USED INDEXES: _____dtf_tasks_vq_cdu_idx
    @Override
    public Set<AffinityGroupWrapper> affinityGroupsInNewVirtualQueue(LocalDateTime from, Duration overlap) {
        return Sets.newHashSet(namedParameterJdbcTemplate.query(
                SELECT_AFFINITY_GROUPS_IN_VIRTUAL_QUEUE,
                SqlParameters.of(
                    "from", from, Types.TIMESTAMP,
                    "timeOverlapSec", overlap.getSeconds(), Types.BIGINT
                ),
                AFFINITY_GROUP_WRAPPER_BEAN_PROPERTY_ROW_MAPPER
            )
        );
    }


    //language=postgresql
    private static final String AFFINITY_GROUP_TABLE_SELECT = """
        WITH params(affinity_group) AS (
            SELECT * FROM unnest(:affinityGroup::text[])
        )
        SELECT 
            p.affinity_group AS affinity_group, 
            c.number AS number
        FROM params p
        CROSS JOIN LATERAL (
            SELECT count(1) AS number
            FROM (
                SELECT 1
                FROM _____dtf_tasks d
                WHERE d.virtual_queue = 'NEW'::_____dtf_virtual_queue_type
                AND d.affinity_group = p.affinity_group
                ORDER BY d.workflow_created_date_utc
                LIMIT :limit
            ) tmp
        ) c
        UNION ALL
        SELECT NULL AS affinity_group, (
            SELECT count(1) FROM (
                SELECT 1
                FROM _____dtf_tasks d
                WHERE d.virtual_queue = 'NEW'::_____dtf_virtual_queue_type
                AND d.affinity_group IS NULL
                ORDER BY d.workflow_created_date_utc
                LIMIT :limit
            ) tmp
        ) AS number
        WHERE :hasNullGroup
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_vq_ag_wcdu_idx
    @Override
    public Set<AffinityGroupStat> affinityGroupInNewVirtualQueueStat(Set<AffinityGroupWrapper> knownAffinityGroups,
                                                                     int affinityGroupLimit) {
        if (knownAffinityGroups.isEmpty()) {
            return Sets.newHashSet();
        }

        boolean hasNullGroup = knownAffinityGroups.contains(AffinityGroupWrapper.EMPTY);
        var affinityGroups = knownAffinityGroups.stream()
            .filter(agw -> !Objects.equals(AffinityGroupWrapper.EMPTY, agw))
            .map(AffinityGroupWrapper::getAffinityGroup)
            .toList();

        return Sets.newHashSet(namedParameterJdbcTemplate.query(
                AFFINITY_GROUP_TABLE_SELECT,
                SqlParameters.of(
                    AffinityGroupWrapper.Fields.affinityGroup, JdbcTools.toArray(affinityGroups), Types.ARRAY,
                    "limit", affinityGroupLimit, Types.BIGINT,
                    "hasNullGroup", hasNullGroup, Types.BOOLEAN
                ),
                AffinityGroupStat.AFFINITY_GROUP_STAT_MAPPER
            )
        );
    }


    //language=postgresql
    private static final String TASKS_FROM_NEW_SELECT = """
        WITH params_with_ag(affinity_group, lim) AS (
            SELECT * FROM unnest(:affinityGroup::text[], :number::int[])
        ),
        new_raw_with_ag AS (
            SELECT t.id, t.affinity_group, t.affinity, t.task_name, t.workflow_id, t.workflow_created_date_utc, t.version
            FROM params_with_ag p
            CROSS JOIN LATERAL (
                SELECT id, affinity_group, affinity, task_name, workflow_id, workflow_created_date_utc, version
                FROM _____dtf_tasks d
                WHERE d.virtual_queue  = 'NEW'::_____dtf_virtual_queue_type
                    AND d.affinity_group = p.affinity_group
                ORDER BY d.workflow_created_date_utc
                LIMIT p.lim
            ) t
        ),
        new_raw_without_ag AS (
            SELECT id, affinity_group, affinity, task_name, workflow_id, workflow_created_date_utc, version
            FROM _____dtf_tasks d
            WHERE :hasNullGroup
                AND d.virtual_queue  = 'NEW'::_____dtf_virtual_queue_type
                AND d.affinity_group IS NULL
            ORDER BY d.workflow_created_date_utc
            LIMIT :nullNumber
        ),
        new_raw_union_portion AS (
            SELECT * FROM new_raw_with_ag
            UNION ALL
            SELECT * FROM new_raw_without_ag
        ),
        new_unique_wid AS (
            SELECT DISTINCT affinity_group, affinity, workflow_id
            FROM new_raw_union_portion
            WHERE 
                affinity_group IS NOT NULL
                AND affinity IS NOT NULL
        ),
        new_raw_portion_extended_by_wid AS (
            SELECT d.id, d.affinity_group, d.affinity, d.task_name, d.workflow_id, d.workflow_created_date_utc, d.version
            FROM new_unique_wid u
            JOIN _____dtf_tasks d
            ON d.affinity_group = u.affinity_group
                AND d.affinity       = u.affinity
                AND d.virtual_queue  = 'NEW'::_____dtf_virtual_queue_type
                AND d.workflow_id    = u.workflow_id
        ),
        new_raw_extended_portion AS (
            SELECT id, affinity_group, affinity, task_name, workflow_id, workflow_created_date_utc, version FROM new_raw_union_portion
            UNION
            SELECT id, affinity_group, affinity, task_name, workflow_id, workflow_created_date_utc, version FROM new_raw_portion_extended_by_wid
        ),
        new_portion AS (
            SELECT 
                id, 
                affinity_group, 
                affinity, 
                task_name, 
                workflow_id, 
                version,
                first_value(workflow_id) OVER (
                    PARTITION BY affinity_group, affinity
                    ORDER BY workflow_created_date_utc, workflow_id
                ) AS min_workflow_id
            FROM new_raw_extended_portion
        ),
        candidate_groups AS (
            SELECT DISTINCT affinity_group, affinity
            FROM new_portion
            WHERE affinity_group IS NOT NULL AND affinity IS NOT NULL
        ),
        occupied_groups AS (
            SELECT g.affinity_group, g.affinity
            FROM candidate_groups g
            WHERE EXISTS (
                SELECT 1 FROM _____dtf_tasks t
                WHERE t.affinity_group = g.affinity_group
                    AND t.affinity       = g.affinity
                    AND t.virtual_queue  = 'READY'::_____dtf_virtual_queue_type
            )
            OR EXISTS (
                SELECT 1 FROM _____dtf_tasks t
                WHERE t.affinity_group = g.affinity_group
                    AND t.affinity       = g.affinity
                    AND t.virtual_queue  = 'PARKED'::_____dtf_virtual_queue_type
            )
        )
        SELECT 
            np.id AS id, 
            np.version AS version,
            CASE
                WHEN np.affinity IS NOT NULL
                    AND np.workflow_id <> np.min_workflow_id
                THEN 'PARKED'::_____dtf_virtual_queue_type
                WHEN np.affinity_group IS NOT NULL
                    AND np.affinity IS NOT NULL
                    AND EXISTS (
                        SELECT 1 FROM occupied_groups o
                        WHERE o.affinity_group = np.affinity_group
                          AND o.affinity       = np.affinity
                    )
                THEN 'PARKED'::_____dtf_virtual_queue_type
                ELSE 'READY'::_____dtf_virtual_queue_type
            END AS virtual_queue
        FROM new_portion np
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_parked_ag_a_wcdu_wid_idx, _____dtf_tasks_ag_a_vq_wid_idx, _____dtf_tasks_vq_ag_wcdu_idx, _____dtf_tasks_wid_idx
    @Override
    public List<IdVersionWithVirtualQueue> getTasksFromNew(Set<AffinityGroupStat> affinityGroupStats) {
        if (affinityGroupStats.isEmpty()) {
            return List.of();
        }

        var affinityGroups = affinityGroupStats.stream()
            .filter(agw -> !Objects.equals(AffinityGroupStat.NULL_GROUP, agw))
            .map(AffinityGroupStat::getAffinityGroup)
            .toList();
        var numbers = affinityGroupStats.stream()
            .filter(agw -> !Objects.equals(AffinityGroupStat.NULL_GROUP, agw))
            .map(AffinityGroupStat::getNumber)
            .toList();
        var nullNumber = affinityGroupStats.stream()
            .filter(agw -> Objects.equals(AffinityGroupStat.NULL_GROUP, agw))
            .findFirst()
            .map(AffinityGroupStat::getNumber)
            .orElse(0);

        return Lists.newArrayList(namedParameterJdbcTemplate.query(
                TASKS_FROM_NEW_SELECT,
                SqlParameters.of(
                    AffinityGroupStat.Fields.affinityGroup, JdbcTools.toArray(affinityGroups), Types.ARRAY,
                    AffinityGroupStat.Fields.number, JdbcTools.toIntegerArray(numbers), Types.ARRAY,
                    "nullNumber", nullNumber, Types.BIGINT,
                    "hasNullGroup", nullNumber > 0, Types.BOOLEAN
                ),
                ID_VERSION_WITH_VIRTUAL_QUEUE_ROW_MAPPER
            )
        );
    }


    //language=postgresql
    private static final String MOVE_NEW_TO_READY_AND_PARKED = """
        WITH to_update AS (
            SELECT id, version, target_virtual_queue
            FROM unnest(:id::uuid[], :version::int[], :virtualQueue::_____dtf_virtual_queue_type[]) AS t(id, version, target_virtual_queue)
        ),
        locked AS (
            SELECT d.id, d.version, c.target_virtual_queue
            FROM to_update c
            JOIN _____dtf_tasks d ON d.id = c.id AND d.version = c.version
            ORDER BY d.id
            FOR NO KEY UPDATE OF d SKIP LOCKED
        )
        UPDATE _____dtf_tasks d
        SET
            virtual_queue = l.target_virtual_queue,
            version = l.version
        FROM locked l
        WHERE d.id = l.id AND d.version = l.version
        RETURNING d.id, d.affinity_group, d.affinity, d.task_name, d.virtual_queue
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_ag_a_vq_idx, _____dtf_tasks_pkey, _____dtf_tasks_vq_ag_wcdu_idx
    @Override
    public List<ShortTaskEntity> moveNewToReadyAndParked(List<IdVersionWithVirtualQueue> idVersionWithVirtualQueues) {
        if (idVersionWithVirtualQueues.isEmpty()) {
            return Collections.emptyList();
        }
        var ids = idVersionWithVirtualQueues.stream().map(IdVersionWithVirtualQueue::getId).toList();
        var versions = idVersionWithVirtualQueues.stream().map(IdVersionWithVirtualQueue::getVersion).toList();
        var virtualQueues = idVersionWithVirtualQueues.stream().map(IdVersionWithVirtualQueue::getVirtualQueue).toList();

        return Lists.newArrayList(namedParameterJdbcTemplate.query(
                MOVE_NEW_TO_READY_AND_PARKED,
                SqlParameters.of(
                    IdVersionWithVirtualQueue.Fields.id, JdbcTools.UUIDsToStringArray(ids), Types.ARRAY,
                    IdVersionWithVirtualQueue.Fields.version, JdbcTools.toLongArray(versions), Types.ARRAY,
                    IdVersionWithVirtualQueue.Fields.virtualQueue, JdbcTools.toEnumArray(virtualQueues), Types.ARRAY
                ),
                ShortTaskEntity.SHORT_TASK_ROW_MAPPER
            )
        );
    }


    //language=postgresql
    private static final String READY_TO_MOVE_FROM_PARKED_TO_READY = """
        WITH deleted_afg_af AS (
            SELECT affinity_group, affinity
            FROM UNNEST(:affinityGroup::text[], :affinity::text[])
            AS tmp(affinity_group, affinity)
        ),
        free_afg_af AS (
            SELECT d.affinity_group, d.affinity
            FROM deleted_afg_af d
            WHERE NOT EXISTS (
                SELECT 1
                FROM _____dtf_tasks t
                WHERE t.affinity_group = d.affinity_group
                AND t.affinity       = d.affinity
                AND t.virtual_queue  = 'READY'
            )
        ),
        free_afg_af_wid AS (
            SELECT t.affinity_group, t.affinity, t.workflow_created_date_utc, t.workflow_id
            FROM free_afg_af f
            CROSS JOIN LATERAL (
                SELECT affinity_group, affinity, workflow_created_date_utc, workflow_id
                FROM _____dtf_tasks
                WHERE affinity_group = f.affinity_group
                AND affinity       = f.affinity
                AND virtual_queue  = 'PARKED'
                ORDER BY workflow_created_date_utc, workflow_id
                LIMIT 1 
            ) t
        )
        SELECT t.id, t.version
        FROM free_afg_af_wid f
        CROSS JOIN LATERAL (
            SELECT id, version
            FROM _____dtf_tasks
            WHERE affinity_group = f.affinity_group
            AND affinity         = f.affinity
            AND virtual_queue    = 'PARKED'
            AND workflow_created_date_utc = f.workflow_created_date_utc 
            AND workflow_id      = f.workflow_id
        ) t
        ORDER BY t.id;
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_parked_ag_a_wcdu_wid_idx, _____dtf_tasks_ag_a_vq_wid_idx
    @Override
    public List<IdVersionEntity> readyToMoveFromParkedToReady(Collection<AffinityGroupAndAffinity> affinityGroupAndAffinities) {
        var affinityGroups = affinityGroupAndAffinities.stream().map(AffinityGroupAndAffinity::affinityGroup).toList();
        var affinities = affinityGroupAndAffinities.stream().map(AffinityGroupAndAffinity::affinity).toList();
        return Lists.newArrayList(namedParameterJdbcTemplate.query(
                READY_TO_MOVE_FROM_PARKED_TO_READY,
                SqlParameters.of(
                    AffinityGroupAndAffinity.Fields.affinityGroup, JdbcTools.toArray(affinityGroups), Types.ARRAY,
                    AffinityGroupAndAffinity.Fields.affinity, JdbcTools.toArray(affinities), Types.ARRAY
                ),
                IdVersionEntity.ID_VERSION_ROW_MAPPER
            )
        );
    }


    private static final String MOVE_PARKED_TO_READY = """
        WITH to_update AS (
            SELECT id, version
            FROM _____dtf_tasks
            WHERE (id, version) IN (
                SELECT id, version
                FROM UNNEST(:id::uuid[], :version::int[]) AS t(id, version)
            )
            ORDER BY id
            FOR UPDATE
        )
        UPDATE _____dtf_tasks dt
        SET
            virtual_queue = 'READY'::_____dtf_virtual_queue_type,
            version = dt.version + 1
        FROM to_update u
        WHERE dt.id = u.id AND dt.version = u.version
        RETURNING
            dt.id,
            dt.task_name,
            dt.version,
            dt.virtual_queue,
            dt.affinity_group,
            dt.affinity
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public List<ShortTaskEntity> moveParkedToReady(Collection<IdVersionEntity> idVersionEntities) {
        List<UUID> ids = idVersionEntities.stream().map(IdVersionEntity::getId).toList();
        List<Long> versions = idVersionEntities.stream().map(IdVersionEntity::getVersion).toList();
        return Lists.newArrayList(namedParameterJdbcTemplate.query(
                MOVE_PARKED_TO_READY,
                SqlParameters.of(
                    IdVersionEntity.Fields.id, JdbcTools.UUIDsToStringArray(ids), Types.ARRAY,
                    IdVersionEntity.Fields.version, JdbcTools.toLongArray(versions), Types.ARRAY
                ),
                ShortTaskEntity.SHORT_TASK_ROW_MAPPER
            )
        );
    }

    //language=postgresql
    private static final String READY_TO_HARD_DELETE = """
        SELECT 
            id, 
            version,
            affinity_group,
            affinity
        FROM _____dtf_tasks
        WHERE virtual_queue = 'DELETED'
        ORDER BY deleted_at
        LIMIT :limit
        """;

    @Override
    public Set<IdVersionWithAffinityEntity> readyToHardDelete(int batchSize) {
        return Sets.newHashSet(namedParameterJdbcTemplate.query(
                READY_TO_HARD_DELETE,
                SqlParameters.of("limit", batchSize, Types.BIGINT),
                IdVersionWithAffinityEntity.ID_VERSION_WITH_AFFINITY_ROW_MAPPER
            )
        );
    }


    //language=postgresql
    private static final String COUNT_BY_VIRTUAL_QUEUE = """
        SELECT count(1)
        FROM _____dtf_tasks
        WHERE virtual_queue = :virtualQueue::_____dtf_virtual_queue_type
        """;

    @SuppressWarnings("DataFlowIssue")
    @Override
    public int countOfTasksInVirtualQueue(VirtualQueue virtualQueue) {
        return namedParameterJdbcTemplate.queryForObject(
            COUNT_BY_VIRTUAL_QUEUE,
            SqlParameters.of(TaskEntity.Fields.virtualQueue, JdbcTools.asString(virtualQueue), Types.VARCHAR),
            Integer.class
        );
    }


    //language=postgresql
    private static final String SOFT_DELETE = """
        UPDATE _____dtf_tasks
        SET
            virtual_queue = :virtualQueue::_____dtf_virtual_queue_type,
            deleted_at = :deletedAt,
            version = :version + 1
        WHERE
        (
            id = :id::uuid
            AND version = :version
        )
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public TaskEntity softDelete(TaskEntity taskEntity) {
        taskEntity = prepareToSoftDelete(taskEntity);
        var updatedRows = namedParameterJdbcTemplate.update(
            SOFT_DELETE,
            toSqlParameterSourceForDeleting(taskEntity)
        );
        if (updatedRows == 1) {
            return taskEntity.toBuilder()
                .version(taskEntity.getVersion() + 1)
                .build();
        }

        UUID taskId = taskEntity.getId();
        if (!taskRepositoryHelper.filerExisted(List.of(taskId)).isEmpty()) {
            throw new OptimisticLockException(format("Can't update=[%s]", taskEntity), TaskEntity.class);
        }
        throw new UnknownTaskException(taskId);
    }


    //language=postgresql
    private static final String SOFT_DELETE_ALL = """
        WITH to_update AS (
            SELECT id, version
            FROM _____dtf_tasks
            WHERE (id, version) IN (
                SELECT id, version
                FROM UNNEST(:id::uuid[], :version::int[]) AS t(id, version)
            )
            ORDER BY id
            FOR NO KEY UPDATE
        )
        UPDATE _____dtf_tasks dt
        SET
            virtual_queue = 'DELETED'::_____dtf_virtual_queue_type,
            deleted_at    = :deletedAt,
            version       = dt.version + 1
        FROM to_update u
        WHERE dt.id = u.id AND dt.version = u.version
        RETURNING dt.id
        """;

    @Override
    public void softDeleteAll(Collection<TaskEntity> taskEntities) {
        if (taskEntities.isEmpty()) {
            return;
        }
        var ids = taskEntities.stream().map(TaskEntity::getId).toList();
        var versions = taskEntities.stream().map(TaskEntity::getVersion).toList();
        var deletedAt = LocalDateTime.now(clock);

        var updatedIds = namedParameterJdbcTemplate.queryForList(
            SOFT_DELETE_ALL,
            SqlParameters.of(
                TaskEntity.Fields.id, JdbcTools.UUIDsToStringArray(ids), Types.ARRAY,
                TaskEntity.Fields.version, JdbcTools.toLongArray(versions), Types.ARRAY,
                TaskEntity.Fields.deletedAt, deletedAt, Types.TIMESTAMP
            ),
            UUID.class
        );

        if (updatedIds.size() == taskEntities.size()) {
            return;
        }

        var updatedIdSet = Sets.newHashSet(updatedIds);
        var notAffectedIds = ids.stream()
            .filter(id -> !updatedIdSet.contains(id))
            .collect(Collectors.toSet());

        var optimisticLockIds = taskRepositoryHelper.filerExisted(notAffectedIds);
        var unknownTaskIds = Sets.difference(notAffectedIds, Sets.newHashSet(optimisticLockIds));

        throw BatchUpdateException.builder()
            .optimisticLockTaskIds(optimisticLockIds)
            .unknownTaskIds(Lists.newArrayList(unknownTaskIds))
            .build();
    }

    private TaskEntity prepareToSoftDelete(TaskEntity taskEntity) {
        return taskEntity.toBuilder()
            .deletedAt(LocalDateTime.now(clock))
            .virtualQueue(VirtualQueue.DELETED)
            .build();
    }

    private MapSqlParameterSource toSqlParameterSourceForDeleting(TaskEntity taskEntity) {
        return SqlParameters.of(
            TaskEntity.Fields.id, JdbcTools.asNullableString(taskEntity.getId()), Types.VARCHAR,
            TaskEntity.Fields.version, taskEntity.getVersion(), Types.BIGINT,
            TaskEntity.Fields.virtualQueue, JdbcTools.asString(taskEntity.getVirtualQueue()), Types.VARCHAR,
            TaskEntity.Fields.deletedAt, taskEntity.getDeletedAt(), Types.TIMESTAMP
        );
    }
}
