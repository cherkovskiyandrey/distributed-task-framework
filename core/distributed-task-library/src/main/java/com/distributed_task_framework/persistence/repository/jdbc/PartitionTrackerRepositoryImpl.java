package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.persistence.repository.PartitionTrackerRepository;
import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.SqlParameters;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.sql.Types;
import java.util.List;
import java.util.Set;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class PartitionTrackerRepositoryImpl implements PartitionTrackerRepository {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;

    public PartitionTrackerRepositoryImpl(DtfJdbcInfrastructure dtfJdbcInfrastructure) {
        this.namedParameterJdbcTemplate = dtfJdbcInfrastructure.getNamedParameterJdbcOperations();
    }

    private static final String SELECT_ACTIVE_PARTITIONS = """
        SELECT distinct affinity_group, task_name
        FROM _____dtf_tasks
        WHERE virtual_queue = 'READY'::_____dtf_virtual_queue_type
        """;

    private static final BeanPropertyRowMapper<Partition> PARTITION_MAPPER = new BeanPropertyRowMapper<>(Partition.class);

    //SUPPOSED USED INDEXES: not use index at all, but check very quickly
    @Override
    public Set<Partition> activePartitions() {
        return Sets.newHashSet(namedParameterJdbcTemplate.query(
                SELECT_ACTIVE_PARTITIONS,
                PARTITION_MAPPER
            )
        );
    }

    //language=postgresql
    private static final String ACTIVE_PARTITIONS_SELECT = """
        WITH filter AS (
            SELECT DISTINCT task_name, affinity_group
            FROM UNNEST(:taskName::varchar[], :affinityGroup::varchar[])
            AS t(task_name, affinity_group)
        )
        SELECT f.task_name, f.affinity_group
        FROM filter f
        WHERE f.affinity_group IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM _____dtf_tasks t
                WHERE t.task_name = f.task_name
                AND t.affinity_group = f.affinity_group
                AND t.virtual_queue = 'READY'
            )
        UNION ALL
        SELECT f.task_name, f.affinity_group
        FROM filter f
        WHERE f.affinity_group IS NULL
            AND EXISTS (
                SELECT 1 FROM _____dtf_tasks t
                WHERE t.task_name = f.task_name
                AND t.affinity_group IS NULL
                AND t.virtual_queue = 'READY'
            )
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_tn_afg_vq_edu_idx
    @Override
    public Set<Partition> filterInReadyVirtualQueue(Set<Partition> entities) {
        List<Partition> partitionList = Lists.newArrayList(entities);
        var taskNames = partitionList.stream().map(Partition::getTaskName).toList();
        var affinityGroups = partitionList.stream().map(Partition::getAffinityGroup).toList();
        return Sets.newHashSet(namedParameterJdbcTemplate.query(
            ACTIVE_PARTITIONS_SELECT,
            SqlParameters.of(
                Partition.Fields.taskName, JdbcTools.toArray(taskNames), Types.ARRAY,
                Partition.Fields.affinityGroup, JdbcTools.toArray(affinityGroups), Types.ARRAY
            ),
            PARTITION_MAPPER
        ));
    }
}
