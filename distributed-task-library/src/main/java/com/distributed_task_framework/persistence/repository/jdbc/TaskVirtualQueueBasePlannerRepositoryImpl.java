package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.model.NodeTaskActivity;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.model.PartitionStat;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.repository.TaskVirtualQueueBasePlannerRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskVirtualQueueBasePlannerRepositoryImpl implements TaskVirtualQueueBasePlannerRepository {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;
    Clock clock;

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

    public TaskVirtualQueueBasePlannerRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcOperations namedParameterJdbcTemplate, Clock clock) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.clock = clock;
    }

    //SUPPOSED USED INDEXES: _____dtf_tasks_aw_idx
    @Override
    public List<NodeTaskActivity> currentAssignedTaskStat(Set<UUID> knownNodes, Set<String> knownTaskNames) {
        return namedParameterJdbcTemplate.query(
                        SELECT_CURRENT_ASSIGNED_TASK_STAT,
                        Map.of(
                                "knownTaskNames", JdbcTools.toArray(knownTaskNames),
                                "knownNodes", JdbcTools.UUIDsToStringArray(knownNodes)
                        ),
                        NODE_TASK_ACTIVITY_ROW_MAPPER
                )
                .stream().toList();
    }


    //language=PostgreSQL
    private static final String PARTITION_STAT_COUNT_SUB_SELECT_TEMPLATE = """
             {TABLE_NAME} AS (
                 SELECT
                    '{TASK_NAME}' AS task_name,
                    {AFFINITY_GROUP} AS affinity_group,
                    count(1) AS number
                 FROM (
                          SELECT 1
                          FROM _____dtf_tasks
                          WHERE virtual_queue = 'READY'
                            AND task_name = '{TASK_NAME}'
                            AND affinity_group {AFFINITY_GROUP_EXPRESSION}
                            AND (not_to_plan = FALSE)
                            AND (
                                  (assigned_worker IS NULL)
                                  OR (NOT (assigned_worker = ANY ((:knownNodes)::uuid[])))
                              )
                            AND (execution_date_utc <= :executionDateUtc)
                          ORDER BY execution_date_utc
                          LIMIT :limit
                      ) tmp
             )
            """;

    //language=PostgreSQL
    private static final String PARTITION_STAT_COUNT_SELECT_TEMPLATE = """
            WITH {PARTITION_STAT_COUNT_SUB_TABLES},
            all_tasks AS ({PARTITION_STAT_AGGREGATED_TABLE})
            SELECT *
            FROM all_tasks;
            """;

    private static final BeanPropertyRowMapper<PartitionStat> TASK_NAME_AFFINITY_GROUP_STAT_MAPPER =
            new BeanPropertyRowMapper<>(PartitionStat.class);

    //SUPPOSED USED INDEXES: _____dtf_tasks_tn_afg_vq_edu_idx
    @Override
    public Set<PartitionStat> findPartitionStatToPlan(Set<UUID> knownNodes,
                                                      Set<Partition> entities,
                                                      int limit) {
        final String tablePrefix = "afg_tn_";
        List<Partition> partitions = Lists.newArrayList(entities);
        String affinityGroupTaskNameTables = IntStream.range(0, partitions.size())
                .mapToObj(i -> {
                            Partition partition = partitions.get(i);
                            return PARTITION_STAT_COUNT_SUB_SELECT_TEMPLATE
                                    .replace("{TABLE_NAME}", tablePrefix + i)
                                    .replace("{TASK_NAME}", partition.getTaskName())
                                    .replace(
                                            "{AFFINITY_GROUP}",
                                            JdbcTools.stringValueOrNullObject(partition.getAffinityGroup())
                                    )
                                    .replace(
                                            "{AFFINITY_GROUP_EXPRESSION}",
                                            JdbcTools.valueOrNullExpression(partition.getAffinityGroup())
                                    )
                                    ;
                        }
                )
                .collect(Collectors.joining(" , "));

        String affinityGroupTaskNameAggregatedTable = JdbcTools.buildCommonAggregatedTable(
                partitions.size(),
                tablePrefix
        );

        String query = PARTITION_STAT_COUNT_SELECT_TEMPLATE
                .replace("{PARTITION_STAT_COUNT_SUB_TABLES}", affinityGroupTaskNameTables)
                .replace("{PARTITION_STAT_AGGREGATED_TABLE}", affinityGroupTaskNameAggregatedTable);


        return Sets.newHashSet(namedParameterJdbcTemplate.query(
                        query,
                        Map.of(
                                "knownNodes", JdbcTools.UUIDsToStringArray(knownNodes),
                                "executionDateUtc", LocalDateTime.now(clock),
                                "limit", limit
                        ),
                        TASK_NAME_AFFINITY_GROUP_STAT_MAPPER
                )
        );
    }


    //language=PostgreSQL
    private static final String TASKS_TO_PLAN_SUB_SELECT_TEMPLATE = """
            {TABLE_NAME} AS (
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
                      FROM _____dtf_tasks
                      WHERE virtual_queue = 'READY'
                        AND task_name = '{TASK_NAME}'
                        AND affinity_group {AFFINITY_GROUP_EXPRESSION}
                        AND (not_to_plan = FALSE)
                        AND (
                              (assigned_worker IS NULL)
                              OR (NOT (assigned_worker = ANY ((:knownNodes)::uuid[])))
                          )
                        AND (execution_date_utc <= :executionDateUtc)
                      ORDER BY execution_date_utc
                      LIMIT {LIMIT}
                 )
            """;

    //language=PostgreSQL
    private static final String TASKS_TO_PLAN_SELECT_TEMPLATE = """
            WITH {TASK_TO_PLAN_SUB_SELECT_TABLES},
            all_tasks AS ({TASK_TO_PLAN_AGGREGATED_TABLE})
            SELECT *
            FROM all_tasks;
            """;

    private static final BeanPropertyRowMapper<ShortTaskEntity> SHORT_TASK_ROW_MAPPER =
            new BeanPropertyRowMapper<>(ShortTaskEntity.class);

    //SUPPOSED USED INDEXES: _____dtf_tasks_tn_afg_vq_edu_idx
    @Override
    public Collection<ShortTaskEntity> loadTasksToPlan(Set<UUID> knownNodes,
                                                       Map<Partition, Integer> partitionToLimits) {
        final String tablePrefix = "afg_tn_";
        List<Partition> partitions = Lists.newArrayList(partitionToLimits.keySet());
        String taskToPlanTables = IntStream.range(0, partitions.size())
                .mapToObj(i -> {
                            Partition partition = partitions.get(i);
                            return TASKS_TO_PLAN_SUB_SELECT_TEMPLATE
                                    .replace("{TABLE_NAME}", tablePrefix + i)
                                    .replace("{TASK_NAME}", partition.getTaskName())
                                    .replace(
                                            "{AFFINITY_GROUP_EXPRESSION}",
                                            JdbcTools.valueOrNullExpression(partition.getAffinityGroup())
                                    )
                                    .replace("{LIMIT}", partitionToLimits.get(partition).toString());
                        }
                )
                .collect(Collectors.joining(" , "));

        String taskToPlanAggregatedTable = JdbcTools.buildCommonAggregatedTable(
                partitions.size(),
                tablePrefix
        );

        String query = TASKS_TO_PLAN_SELECT_TEMPLATE
                .replace("{TASK_TO_PLAN_SUB_SELECT_TABLES}", taskToPlanTables)
                .replace("{TASK_TO_PLAN_AGGREGATED_TABLE}", taskToPlanAggregatedTable);

        return Sets.newHashSet(namedParameterJdbcTemplate.query(
                        query,
                        Map.of(
                                "knownNodes", JdbcTools.UUIDsToStringArray(knownNodes),
                                "executionDateUtc", LocalDateTime.now(clock)
                        ),
                        SHORT_TASK_ROW_MAPPER
                )
        );
    }
}
