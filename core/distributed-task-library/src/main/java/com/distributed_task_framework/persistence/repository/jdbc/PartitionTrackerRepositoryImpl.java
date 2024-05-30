package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.persistence.repository.PartitionTrackerRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
public class PartitionTrackerRepositoryImpl implements PartitionTrackerRepository {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;

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


    private static final String ACTIVE_PARTITIONS_SUB_SELECT_TEMPLATE = """
            {TABLE_NAME} AS (
                SELECT task_name, affinity_group
                FROM _____dtf_tasks
                WHERE virtual_queue = 'READY'
                   AND task_name = '{TASK_NAME}'
                   AND affinity_group {AFFINITY_GROUP_EXPRESSION}
                ORDER BY execution_date_utc
                LIMIT 1
            )
            """;

    private static final String ACTIVE_PARTITIONS_SELECT_TEMPLATE = """
            WITH {ACTIVE_PARTITIONS_FILTER_SUB_TABLES},
            all_tasks AS ({ACTIVE_PARTITIONS_AGGREGATED_TABLE})
            SELECT *
            FROM all_tasks;
            """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_vq_ag_wcdu_idx, _____dtf_tasks_tn_afg_vq_edu_idx
    @Override
    public Set<Partition> filterInReadyVirtualQueue(Set<Partition> entities) {
        final String tablePrefix = "partition_";
        List<Partition> partitionList = Lists.newArrayList(entities);
        String activePartitionTables = IntStream.range(0, partitionList.size())
                .mapToObj(i -> {
                            Partition partition = partitionList.get(i);
                            return ACTIVE_PARTITIONS_SUB_SELECT_TEMPLATE
                                    .replace("{TABLE_NAME}", tablePrefix + i)
                                    .replace("{TASK_NAME}", partition.getTaskName())
                                    .replace(
                                            "{AFFINITY_GROUP_EXPRESSION}",
                                            JdbcTools.valueOrNullExpression(partition.getAffinityGroup())
                                    )
                                    ;

                        }
                )
                .collect(Collectors.joining(" , "));

        String activePartitionsAggregatedTable = JdbcTools.buildCommonAggregatedTable(partitionList.size(), tablePrefix);

        String query = ACTIVE_PARTITIONS_SELECT_TEMPLATE
                .replace("{ACTIVE_PARTITIONS_FILTER_SUB_TABLES}", activePartitionTables)
                .replace("{ACTIVE_PARTITIONS_AGGREGATED_TABLE}", activePartitionsAggregatedTable);

        log.debug("filterInReadyVirtualQueue(): query=[{}]", query);

        return Sets.newHashSet(namedParameterJdbcTemplate.query(query, PARTITION_MAPPER));
    }
}
