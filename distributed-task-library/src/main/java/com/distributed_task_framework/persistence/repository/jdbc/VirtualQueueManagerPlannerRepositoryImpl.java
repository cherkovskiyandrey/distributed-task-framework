package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.model.AffinityGroupStat;
import com.distributed_task_framework.model.AffinityGroupWrapper;
import com.distributed_task_framework.persistence.entity.IdVersionEntity;
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
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.sql.Types;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static java.lang.String.format;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class VirtualQueueManagerPlannerRepositoryImpl implements VirtualQueueManagerPlannerRepository {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;

    public VirtualQueueManagerPlannerRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcOperations namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

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


    private static final String AFFINITY_GROUP_TABLE_SUB_SELECT_TEMPLATE = """
        {TABLE_NAME} AS (
        	SELECT {AFFINITY_GROUP} AS affinity_group_name, count(1) AS number
        	FROM (
        		SELECT 1
        		FROM _____dtf_tasks
        		WHERE
        			virtual_queue = 'NEW'::_____dtf_virtual_queue_type
        			AND affinity_group {AFFINITY_GROUP_EXPRESSION}
        		ORDER BY workflow_created_date_utc
        		limit :limit
        	) tmp
        )
        """;

    private static final String SELECT_AFFINITY_GROUP_IN_NEW_VIRTUAL_QUEUE_TEMPLATE = """
        WITH {AFFINITY_GROUP_TABLES},
        agg AS ({AFFINITY_GROUP_AGGREGATED_TABLE})
        SELECT
            affinity_group_name, number
        FROM agg
        """;

    private static final BeanPropertyRowMapper<AffinityGroupStat> AFFINITY_GROUP_STAT_MAPPER =
        new BeanPropertyRowMapper<>(AffinityGroupStat.class);

    //SUPPOSED USED INDEXES: _____dtf_tasks_vq_ag_wcdu_idx
    @Override
    public Set<AffinityGroupStat> affinityGroupInNewVirtualQueueStat(Set<AffinityGroupWrapper> knownAffinityGroups,
                                                                     int affinityGroupLimit) {
        if (knownAffinityGroups.isEmpty()) {
            return Sets.newHashSet();
        }
        String tablePrefix = "afg_";

        List<AffinityGroupWrapper> knownAffinityGroupLst = Lists.newArrayList(knownAffinityGroups);
        String affinityGroupTables = IntStream.range(0, knownAffinityGroupLst.size())
            .mapToObj(i -> AFFINITY_GROUP_TABLE_SUB_SELECT_TEMPLATE
                .replace("{TABLE_NAME}", tablePrefix + i)
                .replace("{AFFINITY_GROUP}", JdbcTools.stringValueOrNullObject(knownAffinityGroupLst.get(i).getAffinityGroup()))
                .replace(
                    "{AFFINITY_GROUP_EXPRESSION}",
                    JdbcTools.valueOrNullExpression(knownAffinityGroupLst.get(i).getAffinityGroup())
                )
            )
            .collect(Collectors.joining(", "));

        String affinityGroupAggregatedTable = JdbcTools.buildCommonAggregatedTable(knownAffinityGroupLst.size(), tablePrefix);
        String query = SELECT_AFFINITY_GROUP_IN_NEW_VIRTUAL_QUEUE_TEMPLATE
            .replace("{AFFINITY_GROUP_TABLES}", affinityGroupTables)
            .replace("{AFFINITY_GROUP_AGGREGATED_TABLE}", affinityGroupAggregatedTable);

        log.debug("affinityGroupInNewVirtualQueueStat(): query=[{}]", query);

        return Sets.newHashSet(namedParameterJdbcTemplate.query(
                query,
                SqlParameters.of("limit", affinityGroupLimit, Types.BIGINT),
                AFFINITY_GROUP_STAT_MAPPER
            )
        );
    }


    //language=PostgreSQL
    private static final String SELECT_AFFINITY_GROUP_NEW_PORTION_TEMPLATE = """
        {TABLE_NAME} AS (
          SELECT
              id,
              affinity_group,
              affinity,
              task_name,
              workflow_id,
              workflow_created_date_utc,
              version
          FROM _____dtf_tasks
          WHERE virtual_queue = 'NEW'
          AND affinity_group {AFFINITY_GROUP_EXPRESSION}
          ORDER BY workflow_created_date_utc
          LIMIT {LIMIT}
        )
        """;


    //language=PostgreSQL
    private static final String MOVE_NEW_TO_READY_TEMPLATE = """
        WITH {AFFINITY_GROUP_TABLES},
        new_raw_union_portion AS ({AGGREGATED_AFFINITY_GROUP_TABLE}),
        new_unique_wid AS (
        	SELECT distinct affinity_group, affinity, workflow_id
        	FROM new_raw_union_portion
        	WHERE affinity_group NOTNULL AND affinity NOTNULL
        ),
        new_raw_portion_extended_by_wid AS (
        	SELECT
              id,
              affinity_group,
              affinity,
              task_name,
              workflow_id,
              workflow_created_date_utc,
              version
        	FROM _____dtf_tasks
        	WHERE (virtual_queue, affinity_group, affinity, workflow_id) IN (
        		SELECT 'NEW'::_____dtf_virtual_queue_type, affinity_group, affinity, workflow_id
        		FROM new_unique_wid
        	)
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
        to_park_base_on_ready AS (
        	SELECT id, version
        	FROM new_portion
        	WHERE
        		affinity_group NOTNULL AND affinity NOTNULL
        		AND
        			(affinity_group, affinity, 'READY') IN (
                    	SELECT affinity_group, affinity, virtual_queue
                        FROM _____dtf_tasks
        			)
        ),
        to_park_base_on_parked AS (
        	SELECT id, version
        	FROM new_portion
        	WHERE
        		affinity_group NOTNULL AND affinity NOTNULL
        		AND
        			(affinity_group, affinity, 'PARKED') IN (
                    	SELECT affinity_group, affinity, virtual_queue
                        FROM _____dtf_tasks
        			)
        ),
        to_park_by_min_workflow_id AS (
            SELECT id, version
            FROM new_portion
            WHERE workflow_id != min_workflow_id AND affinity NOTNULL
        ),
        to_park AS (
            SELECT id, version, 'PARKED'::_____dtf_virtual_queue_type AS target_virtual_queue FROM to_park_base_on_ready
            UNION
            SELECT id, version, 'PARKED'::_____dtf_virtual_queue_type AS target_virtual_queue FROM to_park_base_on_parked
            UNION
            SELECT id, version, 'PARKED'::_____dtf_virtual_queue_type AS target_virtual_queue FROM to_park_by_min_workflow_id
        ),
        to_ready AS (
            SELECT id, version, 'READY'::_____dtf_virtual_queue_type AS target_virtual_queue FROM new_portion
            EXCEPT
            SELECT id, version, 'READY'::_____dtf_virtual_queue_type AS target_virtual_queue FROM to_park
        ),
        patched_new AS (
            SELECT id, version, target_virtual_queue FROM to_park
            UNION all
            SELECT id, version, target_virtual_queue FROM to_ready
        )
        UPDATE _____dtf_tasks d
        SET
            virtual_queue = (SELECT target_virtual_queue FROM patched_new WHERE id = d.id),
            version = version + 1
        WHERE (d.id, d.version) IN (SELECT id, version FROM patched_new ORDER BY id FOR NO KEY UPDATE SKIP LOCKED)
        RETURNING d.id, d.affinity_group, d.affinity, d.task_name, virtual_queue
        """;

    private static final BeanPropertyRowMapper<ShortTaskEntity> SHORT_TASK_ROW_MAPPER =
        new BeanPropertyRowMapper<>(ShortTaskEntity.class);

    //SUPPOSED USED INDEXES: _____dtf_tasks_ag_a_vq_idx, _____dtf_tasks_pkey, _____dtf_tasks_vq_ag_wcdu_idx
    @Override
    public List<ShortTaskEntity> moveNewToReady(Set<AffinityGroupStat> affinityGroupStats) {
        if (affinityGroupStats.isEmpty()) {
            return Collections.emptyList();
        }
        final String tablePrefix = "new_raw_portion_";

        List<AffinityGroupStat> affinityGroupStatLst = Lists.newArrayList(affinityGroupStats);
        String affinityGroupTables = IntStream.range(0, affinityGroupStatLst.size())
            .mapToObj(i -> {
                    var affinityGroupStat = affinityGroupStatLst.get(i);
                    return SELECT_AFFINITY_GROUP_NEW_PORTION_TEMPLATE
                        .replace("{TABLE_NAME}", tablePrefix + i)
                        .replace(
                            "{AFFINITY_GROUP_EXPRESSION}",
                            JdbcTools.valueOrNullExpression(affinityGroupStat.getAffinityGroupName())
                        )
                        .replace("{LIMIT}", affinityGroupStat.getNumber().toString());
                }
            )
            .collect(Collectors.joining(" , "));

        String affinityGroupAggregatedTable = JdbcTools.buildCommonAggregatedTable(affinityGroupStatLst.size(), tablePrefix);

        String query = MOVE_NEW_TO_READY_TEMPLATE
            .replace("{AFFINITY_GROUP_TABLES}", affinityGroupTables)
            .replace("{AGGREGATED_AFFINITY_GROUP_TABLE}", affinityGroupAggregatedTable);

        log.debug("moveNewToReady(): query=[{}]", query);

        return Lists.newArrayList(namedParameterJdbcTemplate.query(query, SHORT_TASK_ROW_MAPPER));
    }


    private static final String MOVE_PARKED_TO_READY = """
        WITH filter AS (
            SELECT id, version
            FROM UNNEST(:id::uuid[], :version::int[])
            AS tmp(id, version)
        ),
        deleted_portion AS (
            SELECT
                id,
                affinity_group,
                affinity,
                virtual_queue AS target_virtual_queue
            FROM _____dtf_tasks
            WHERE
                (id, version) IN (
                    SELECT id, version
                    FROM filter
                )
                AND affinity_group NOTNULL
        ),
        deleted_afg_af AS (
            SELECT distinct affinity_group, affinity
            FROM deleted_portion
        ),
        is_in_ready AS (
                SELECT distinct affinity_group, affinity
                FROM deleted_afg_af
                WHERE
                    (affinity_group, affinity, 'READY') IN (
                        SELECT affinity_group, affinity, virtual_queue
                        FROM _____dtf_tasks
                )
        ),
        free_afg_af AS (
            SELECT affinity_group, affinity FROM deleted_afg_af
            EXCEPT
            SELECT affinity_group, affinity FROM is_in_ready
        ),
        to_unpark_wcd AS (
            SELECT
                affinity_group,
                affinity,
                first_value(workflow_id) OVER (PARTITION BY
                    affinity_group, affinity
                    ORDER BY workflow_created_date_utc, workflow_id
                    ) AS min_workflow_id
            FROM _____dtf_tasks
            WHERE (affinity_group, affinity, virtual_queue) IN
                  (
                      SELECT distinct affinity_group, affinity, 'PARKED'::_____dtf_virtual_queue_type
                      FROM free_afg_af
                  )
        ),
        to_unpark AS (
            SELECT
                id,
                affinity_group,
                affinity,
                virtual_queue,
                version
            FROM _____dtf_tasks
            WHERE (affinity_group, affinity, virtual_queue, workflow_id) IN
                  (
                      SELECT affinity_group, affinity, 'PARKED'::_____dtf_virtual_queue_type, min_workflow_id
                      FROM to_unpark_wcd
                  )
        )
        UPDATE _____dtf_tasks dt
        SET
            virtual_queue = 'READY'::_____dtf_virtual_queue_type,
            version = version + 1
        WHERE (dt.id, dt.version) IN (SELECT id, version FROM to_unpark ORDER BY id FOR NO KEY UPDATE SKIP LOCKED)
        RETURNING dt.id, dt.task_name, dt.version, 'READY'::_____dtf_virtual_queue_type AS virtual_queue, dt.affinity_group, dt.affinity;
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey, _____dtf_tasks_ag_a_vq_idx, _____dtf_tasks_vq_ag_wcdu_idx, _____dtf_tasks_da_vq_idx
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
                SHORT_TASK_ROW_MAPPER
            )
        );
    }


    private static final String READY_TO_HARD_DELETE = """
        SELECT id, version
        FROM _____dtf_tasks
        WHERE virtual_queue = 'DELETED'
        ORDER BY deleted_at
        LIMIT :limit
        """;

    @Override
    public Set<IdVersionEntity> readyToHardDelete(int batchSize) {
        return Sets.newHashSet(namedParameterJdbcTemplate.query(
                READY_TO_HARD_DELETE,
                SqlParameters.of("limit", batchSize, Types.BIGINT),
                IdVersionEntity.ID_VERSION_ROW_MAPPER
            )
        );
    }

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
            SqlParameters.of("virtualQueue", JdbcTools.asString(virtualQueue), Types.VARCHAR),
            Integer.class
        );
    }

    private static final String SOFT_DELETE = """
        UPDATE _____dtf_tasks
        SET
            virtual_queue = 'DELETED'::_____dtf_virtual_queue_type,
            deleted_at = now(),
            version = :version + 1
        WHERE
        (
            id = :id::uuid
            AND version = :version
        )
        """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void softDelete(TaskEntity taskEntity) {
        int updatedRows = namedParameterJdbcTemplate.update(
            SOFT_DELETE,
            SqlParameters.of(
                TaskEntity.Fields.id, JdbcTools.asNullableString(taskEntity.getId()), Types.VARCHAR,
                TaskEntity.Fields.version, taskEntity.getVersion(), Types.BIGINT
            )
        );
        if (updatedRows == 0) {
            throw new OptimisticLockException(format("Can't update=[%s]", taskEntity), TaskEntity.class);
        }
    }
}
