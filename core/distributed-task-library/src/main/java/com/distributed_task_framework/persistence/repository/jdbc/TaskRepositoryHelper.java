package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.SqlParameters;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskRepositoryHelper {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;

    public TaskRepositoryHelper(DtfJdbcInfrastructure dtfJdbcInfrastructure) {
        this.namedParameterJdbcTemplate = dtfJdbcInfrastructure.getNamedParameterJdbcOperations();
    }

    //language=postgresql
    private static final String FILTER_EXISTED = """
        SELECT id
        FROM _____dtf_tasks
        WHERE
            id = ANY( (:taskIds)::uuid[] )
            AND deleted_at IS NULL
        """;

    public List<UUID> filerExisted(Collection<UUID> taskIds) {
        return namedParameterJdbcTemplate.queryForList(
            FILTER_EXISTED,
            SqlParameters.of("taskIds", JdbcTools.UUIDsToStringArray(taskIds), Types.ARRAY),
            UUID.class
        );
    }
}
