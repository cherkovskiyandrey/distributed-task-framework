package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.model.AggregatedTaskStat;
import com.distributed_task_framework.persistence.repository.TaskStatRepository;
import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.SqlParameters;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.sql.Types;
import java.util.List;
import java.util.Set;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskStatRepositoryImpl implements TaskStatRepository {
    NamedParameterJdbcOperations dtfNamedParameterJdbcTemplate;

    public TaskStatRepositoryImpl(DtfJdbcInfrastructure dtfJdbcInfrastructure) {
        this.dtfNamedParameterJdbcTemplate = dtfJdbcInfrastructure.getNamedParameterJdbcOperations();
    }

    //language=postgresql
    private static final String SELECT_AGGREGATED_TASK_STAT = """
        SELECT
            coalesce(affinity_group, 'default') as affinity_group_name,
            task_name,
            not_to_plan as not_to_plan_flag,
            virtual_queue,
            count(*) as number
        FROM _____dtf_tasks
        WHERE
        (
            task_name = ANY(:knownTaskNames)
        )
        GROUP BY affinity_group_name, task_name, not_to_plan_flag, virtual_queue
        """;
    private static final BeanPropertyRowMapper<AggregatedTaskStat> AGGREGATED_TASK_STAT_ROW_MAPPER =
        new BeanPropertyRowMapper<>(AggregatedTaskStat.class);

    //SUPPOSED USED INDEXES: _____dtf_tasks_s_idx or _____dtf_tasks_tn_afg_vq_edu_idx
    @Override
    public List<AggregatedTaskStat> getAggregatedTaskStat(Set<String> knownTaskNames) {
        return dtfNamedParameterJdbcTemplate.query(
                SELECT_AGGREGATED_TASK_STAT,
                SqlParameters.of(
                    "knownTaskNames", JdbcTools.toArray(knownTaskNames), Types.ARRAY
                ),
                AGGREGATED_TASK_STAT_ROW_MAPPER
            )
            .stream().toList();
    }
}
