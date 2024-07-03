package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.repository.TaskWorkerRepository;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.utils.JdbcTools;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskWorkerRepositoryImpl implements TaskWorkerRepository {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;
    CommonSettings commonSettings;

    private static final String SELECT_NEXT_ASSIGNED_TASKS = """
            SELECT *
            FROM _____dtf_tasks
            WHERE
            (
                assigned_worker = :workerId
                AND NOT (id = ANY( (:skippedTasks)::uuid[] ))
                AND deleted_at ISNULL
            )
            ORDER BY execution_date_utc
            LIMIT :maxSize
            """;

    public TaskWorkerRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcOperations namedParameterJdbcTemplate, CommonSettings commonSettings) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.commonSettings = commonSettings;
    }

    //SUPPOSED USED INDEXES: _____dtf_tasks_aw_idx
    @Override
    public Collection<TaskEntity> getNextTasks(UUID workerId, Set<TaskId> skippedTasks, int maxSize) {
        return namedParameterJdbcTemplate.query(
                SELECT_NEXT_ASSIGNED_TASKS,
                Map.of(
                        "workerId", workerId,
                        "skippedTasks", JdbcTools.UUIDsToStringArray(taskIdsToUuds(skippedTasks)),
                        "maxSize", maxSize
                ),
                new BeanPropertyRowMapper<>(TaskEntity.class)
        ).stream().toList();
    }

    private static final String FILTER_CANCELED = """
            SELECT id, task_name
            FROM _____dtf_tasks
            WHERE id = ANY( (:ids)::uuid[] )
            AND canceled = TRUE
            AND deleted_at ISNULL
            """;

    private static final Function<CommonSettings, RowMapper<TaskId>> TASK_ID_ROW_MAPPER_PROVIDER = cs ->
            (rs, rowNum) -> TaskId.builder()
                    .id(rs.getObject("id", UUID.class))
                    .taskName(rs.getString("task_name"))
                    .appName(cs.getAppName())
                    .build();

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public Set<TaskId> filterCanceled(Set<TaskId> taskIds) {
        return new HashSet<>(namedParameterJdbcTemplate.query(
                FILTER_CANCELED,
                Map.of("ids", JdbcTools.UUIDsToStringArray(taskIdsToUuds(taskIds))),
                TASK_ID_ROW_MAPPER_PROVIDER.apply(commonSettings)
        ));
    }

    private Set<UUID> taskIdsToUuds(Collection<TaskId> taskIds) {
        return taskIds.stream().map(TaskId::getId).collect(Collectors.toSet());
    }
}
