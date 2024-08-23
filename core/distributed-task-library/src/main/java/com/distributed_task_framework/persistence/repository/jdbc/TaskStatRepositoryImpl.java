package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.model.AggregatedTaskStat;
import com.distributed_task_framework.persistence.repository.TaskStatRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.SqlParameters;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskStatRepositoryImpl implements TaskStatRepository {
    NamedParameterJdbcOperations dtfNamedParameterJdbcTemplate;

    public TaskStatRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcOperations dtfNamedParameterJdbcTemplate) {
        this.dtfNamedParameterJdbcTemplate = dtfNamedParameterJdbcTemplate;
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

    //SUPPOSED USED INDEXES: don't use any indexes because of scan all history
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
