package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.model.NodeTaskActivity;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.model.PartitionStat;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.repository.TaskVirtualQueueBasePlannerRepository;
import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.SqlParameters;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.sql.Types;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskVirtualQueueBasePlannerRepositoryImpl implements TaskVirtualQueueBasePlannerRepository {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;
    Clock clock;

    public TaskVirtualQueueBasePlannerRepositoryImpl(DtfJdbcInfrastructure dtfJdbcInfrastructure, Clock clock) {
        this.namedParameterJdbcTemplate = dtfJdbcInfrastructure.getNamedParameterJdbcOperations();
        this.clock = clock;
    }

    private static final String SELECT_CURRENT_ASSIGNED_TASK_STAT = """
        SELECT
                task_name as task,
                assigned_worker as node,
                count(*) as number
        FROM _____dtf_tasks
        WHERE
        (
            assigned_worker = ANY((:knownNodes)::uuid[])
            AND (task_name = ANY(:knownTaskNames))
            AND deleted_at ISNULL
        )
        GROUP BY task_name, assigned_worker
        """;
    private static final BeanPropertyRowMapper<NodeTaskActivity> NODE_TASK_ACTIVITY_ROW_MAPPER =
        new BeanPropertyRowMapper<>(NodeTaskActivity.class);

    //SUPPOSED USED INDEXES: _____dtf_tasks_aw_idx
    @Override
    public List<NodeTaskActivity> currentAssignedTaskStat(Set<UUID> knownNodes, Set<String> knownTaskNames) {
        return namedParameterJdbcTemplate.query(
                SELECT_CURRENT_ASSIGNED_TASK_STAT,
                SqlParameters.of(
                    "knownTaskNames", JdbcTools.toArray(knownTaskNames), Types.ARRAY,
                    "knownNodes", JdbcTools.UUIDsToStringArray(knownNodes), Types.ARRAY
                ),
                NODE_TASK_ACTIVITY_ROW_MAPPER
            )
            .stream().toList();
    }


    //language=PostgreSQL
    private static final String PARTITION_STAT_COUNT_SELECT = """
        WITH 
        params_with_afg(task_name, affinity_group) AS (
            SELECT * FROM unnest(:taskNamesWithAfg::text[], :affinityGroups::text[])
        ),
        params_without_afg(task_name) AS (
            SELECT * FROM unnest(:taskNamesWithoutAfg::text[])
        )
        SELECT 
            task_name, 
            affinity_group, 
            number
        FROM (
            SELECT 
                p.task_name, 
                p.affinity_group, 
                c.number
            FROM params_with_afg p
            CROSS JOIN LATERAL (
                SELECT count(1) AS number
                FROM (
                    SELECT 1
                    FROM _____dtf_tasks d
                    WHERE
                    (    
                        d.virtual_queue = 'READY'
                        AND d.task_name = p.task_name
                        AND d.affinity_group = p.affinity_group
                        AND d.not_to_plan = FALSE
                        AND 
                        (
                            d.assigned_worker IS NULL
                            OR NOT (d.assigned_worker = ANY ((:knownNodes)::uuid[]))
                        )
                        AND d.execution_date_utc <= :executionDateUtc
                    )
                    ORDER BY d.execution_date_utc
                    LIMIT :limit
                ) tmp
            ) c
            UNION ALL
            SELECT 
                p.task_name, 
                NULL::text AS affinity_group, 
                c.number
            FROM params_without_afg p
            CROSS JOIN LATERAL (
                SELECT count(1) AS number
                FROM (
                    SELECT 1
                    FROM _____dtf_tasks d
                    WHERE 
                    (
                        d.virtual_queue = 'READY'
                        AND d.task_name = p.task_name
                        AND d.affinity_group IS NULL
                        AND d.not_to_plan = FALSE
                        AND 
                        (
                            d.assigned_worker IS NULL
                            OR NOT (d.assigned_worker = ANY ((:knownNodes)::uuid[]))
                        )
                        AND d.execution_date_utc <= :executionDateUtc
                    )
                    ORDER BY d.execution_date_utc
                    LIMIT :limit
                ) tmp
            ) c
        ) stat
        """;

    private static final BeanPropertyRowMapper<PartitionStat> TASK_NAME_AFFINITY_GROUP_STAT_MAPPER =
        new BeanPropertyRowMapper<>(PartitionStat.class);

    //SUPPOSED USED INDEXES: _____dtf_tasks_tn_afg_vq_edu_idx
    @Override
    public Set<PartitionStat> findPartitionStatToPlan(Set<UUID> knownNodes,
                                                      Set<Partition> entities,
                                                      int limit) {
        if (entities.isEmpty()) {
            return Sets.newHashSet();
        }

        var partitions = entities.stream().collect(Collectors.partitioningBy(p -> p.getAffinityGroup() != null));
        var partitionsWithAfg = partitions.get(true);
        var taskNamesWithAfg = partitionsWithAfg.stream().map(Partition::getTaskName).toList();
        var affinityGroups = partitionsWithAfg.stream().map(Partition::getAffinityGroup).toList();

        var partitionsWithoutAfg = partitions.get(false);
        var taskNamesWithoutAfg = partitionsWithoutAfg.stream().map(Partition::getTaskName).toList();
        return Sets.newHashSet(namedParameterJdbcTemplate.query(
            PARTITION_STAT_COUNT_SELECT,
                SqlParameters.of(
                    "taskNamesWithAfg", JdbcTools.toArray(taskNamesWithAfg), Types.ARRAY,
                    "affinityGroups", JdbcTools.toArray(affinityGroups), Types.ARRAY,
                    "taskNamesWithoutAfg", JdbcTools.toArray(taskNamesWithoutAfg), Types.ARRAY,
                    "knownNodes", JdbcTools.UUIDsToStringArray(knownNodes), Types.ARRAY,
                    "executionDateUtc", LocalDateTime.now(clock), Types.TIMESTAMP,
                    "limit", limit, Types.BIGINT
                ),
                TASK_NAME_AFFINITY_GROUP_STAT_MAPPER
            )
        );
    }


    //language=PostgreSQL
    private static final String TASKS_TO_PLAN_SELECT = """
        WITH
        params_with_afg(task_name, affinity_group, lim) AS (
            SELECT * FROM unnest(:taskNamesWithAfg::text[], :affinityGroups::text[], :limitsWithAfg::int[])
        ),
        params_without_afg(task_name, lim) AS (
            SELECT * FROM unnest(:taskNamesWithoutAfg::text[], :limitsWithoutAfg::int[])
        )
        SELECT 
            id, 
            task_name, 
            version, 
            workflow_id, 
            affinity, 
            affinity_group,
            created_date_utc, 
            assigned_worker, 
            last_assigned_date_utc, 
            execution_date_utc
        FROM (
            SELECT t.*
            FROM params_with_afg p
            CROSS JOIN LATERAL (
                SELECT 
                    d.id, 
                    d.task_name, 
                    d.version, 
                    d.workflow_id, 
                    d.affinity, 
                    d.affinity_group,
                    d.created_date_utc,
                    d.assigned_worker,
                    d.last_assigned_date_utc,
                    d.execution_date_utc
                FROM _____dtf_tasks d
                WHERE d.virtual_queue = 'READY'
                AND d.task_name = p.task_name
                AND d.affinity_group = p.affinity_group
                AND d.not_to_plan = FALSE
                AND 
                (
                    d.assigned_worker IS NULL
                    OR NOT (d.assigned_worker = ANY ((:knownNodes)::uuid[]))
                )
                AND d.execution_date_utc <= :executionDateUtc
                ORDER BY d.execution_date_utc
                LIMIT p.lim
            ) t
            UNION ALL
            SELECT t.*
            FROM params_without_afg p
            CROSS JOIN LATERAL (
                SELECT 
                    d.id, 
                    d.task_name, 
                    d.version, 
                    d.workflow_id, 
                    d.affinity, 
                    d.affinity_group,
                    d.created_date_utc,
                    d.assigned_worker,
                    d.last_assigned_date_utc,
                    d.execution_date_utc
                FROM _____dtf_tasks d
                WHERE d.virtual_queue = 'READY'
                AND d.task_name = p.task_name
                AND d.affinity_group IS NULL
                AND d.not_to_plan = FALSE
                AND 
                (
                    d.assigned_worker IS NULL
                    OR NOT (d.assigned_worker = ANY ((:knownNodes)::uuid[]))
                )
                AND d.execution_date_utc <= :executionDateUtc
                ORDER BY d.execution_date_utc
                LIMIT p.lim
            ) t
        ) all_tasks
        """;

    private static final BeanPropertyRowMapper<ShortTaskEntity> SHORT_TASK_ROW_MAPPER =
        new BeanPropertyRowMapper<>(ShortTaskEntity.class);

    //SUPPOSED USED INDEXES: _____dtf_tasks_tn_afg_vq_edu_idx
    @Override
    public Collection<ShortTaskEntity> loadTasksToPlan(Set<UUID> knownNodes,
                                                       Map<Partition, Integer> partitionToLimits) {
        if (partitionToLimits.isEmpty()) {
            return Sets.newHashSet();
        }

        var partitions = partitionToLimits.entrySet().stream()
            .collect(Collectors.partitioningBy(entry -> entry.getKey().getAffinityGroup() != null));
        var partitionsWithAfg = partitions.get(true);
        var taskNamesWithAfg = partitionsWithAfg.stream().map(Map.Entry::getKey).map(Partition::getTaskName).toList();
        var affinityGroups = partitionsWithAfg.stream().map(Map.Entry::getKey).map(Partition::getAffinityGroup).toList();
        var limitsWithAfg = partitionsWithAfg.stream().map(Map.Entry::getValue).toList();

        var partitionsWithoutAfg = partitions.get(false);
        var taskNamesWithoutAfg = partitionsWithoutAfg.stream().map(Map.Entry::getKey).map(Partition::getTaskName).toList();
        var limitsWithoutAfg = partitionsWithoutAfg.stream().map(Map.Entry::getValue).toList();
        return Sets.newHashSet(namedParameterJdbcTemplate.query(
            TASKS_TO_PLAN_SELECT,
                SqlParameters.of(
                    "taskNamesWithAfg", JdbcTools.toArray(taskNamesWithAfg), Types.ARRAY,
                    "affinityGroups", JdbcTools.toArray(affinityGroups), Types.ARRAY,
                    "taskNamesWithoutAfg", JdbcTools.toArray(taskNamesWithoutAfg), Types.ARRAY,
                    "limitsWithAfg", JdbcTools.toIntegerArray(limitsWithAfg), Types.ARRAY,
                    "limitsWithoutAfg", JdbcTools.toIntegerArray(limitsWithoutAfg), Types.ARRAY,
                    "knownNodes", JdbcTools.UUIDsToStringArray(knownNodes), Types.ARRAY,
                    "executionDateUtc", LocalDateTime.now(clock), Types.TIMESTAMP
                ),
                SHORT_TASK_ROW_MAPPER
            )
        );
    }
}
