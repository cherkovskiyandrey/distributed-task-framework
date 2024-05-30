package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.model.AffinityGroupStat;
import com.distributed_task_framework.model.AffinityGroupWrapper;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.VirtualQueueManagerPlannerRepository;
import com.distributed_task_framework.utils.JdbcTools;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class VirtualQueueManagerPlannerRepositoryImpl implements VirtualQueueManagerPlannerRepository {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;

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
                        Map.of(
                                "from", from,
                                "timeOverlapSec", overlap.getSeconds()
                        ),
                        AFFINITY_GROUP_WRAPPER_BEAN_PROPERTY_ROW_MAPPER
                )
        );
    }

    //language=postgresql
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

    //language=postgresql
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
                        Map.of("limit", affinityGroupLimit),
                        AFFINITY_GROUP_STAT_MAPPER
                )
        );
    }

    //language=postgresql
    private static final String SELECT_AFFINITY_GROUP_NEW_PORTION_TEMPLATE = """
            {TABLE_NAME} AS (
              SELECT
                  id,
                  affinity_group,
                  affinity,
                  task_name,
                  workflow_id,
                  workflow_created_date_utc
              FROM _____dtf_tasks
              WHERE virtual_queue = 'NEW'
              AND affinity_group {AFFINITY_GROUP_EXPRESSION}
              ORDER BY workflow_created_date_utc
              LIMIT {LIMIT}
            )
            """;

    //todo: upper bound for key words
    //language=postgresql
    private static final String MOVE_NEW_TO_READY_TEMPLATE = """
            WITH {AFFINITY_GROUP_TABLES},
            new_raw_union_portion AS ({AGGREGATED_AFFINITY_GROUP_TABLE}),
            new_unique_without_afg_and_with_af_wid as (
            	select distinct affinity, workflow_id
            	from new_raw_union_portion
            	where affinity_group isnull and affinity notnull
            ),
            new_unique_with_afg_and_with_aff_wid as (
            	select distinct affinity_group, affinity, workflow_id
            	from new_raw_union_portion
            	where affinity_group notnull and affinity notnull
            ),
            new_raw_extended_portion_without_afg_and_with_aff as (
            	select *
            	from _____dtf_tasks
            	where (virtual_queue, affinity, workflow_id) in (
            		select 'NEW'::_____dtf_virtual_queue_type, affinity, workflow_id
            		from new_unique_without_afg_and_with_af_wid
            	)
            	and affinity_group isnull
            ),
            new_raw_extended_portion_with_affg_and_with_aff as (
            	select *
            	from _____dtf_tasks
            	where (virtual_queue, affinity_group, affinity, workflow_id) in (
            		select 'NEW'::_____dtf_virtual_queue_type, affinity_group, affinity, workflow_id
            		from new_unique_with_afg_and_with_aff_wid
            	)
            ),
            new_raw_extended_portion as (
            	select id, affinity_group, affinity, task_name, workflow_id, workflow_created_date_utc from new_raw_union_portion
            	union
            	select id, affinity_group, affinity, task_name, workflow_id, workflow_created_date_utc from new_raw_extended_portion_without_afg_and_with_aff
            	union
            	select id, affinity_group, affinity, task_name, workflow_id, workflow_created_date_utc from new_raw_extended_portion_with_affg_and_with_aff
            ),
            new_portion AS (
                SELECT
                    id,
                    affinity_group,
                    affinity,
                    task_name,
                    workflow_id,
                    first_value(workflow_id) OVER (
                        PARTITION BY affinity_group, affinity
                        ORDER BY workflow_created_date_utc, workflow_id
                        ) AS min_workflow_id
                FROM new_raw_extended_portion
            ),
            to_park_without_affinity_group_and_with_affinity_in_ready AS (
            	select distinct id
            	from new_portion
            	where
            		affinity_group ISNULL and affinity notnull
            		and
            			(affinity, 'READY') in (
                        	select affinity, virtual_queue
                            from _____dtf_tasks
                            where affinity_group isnull -- todo: potentially doesn't work because null <> null, CHECH PERFORMANCE
            			)
            ),
            to_park_without_affinity_group_and_with_affinity_in_parked AS (
            	select distinct id
            	from new_portion
            	where
            		affinity_group ISNULL and affinity notnull
            		and
            			(affinity, 'PARKED') in (
                        	select affinity, virtual_queue
                            from _____dtf_tasks
                            where affinity_group isnull -- todo: potentially doesn't work because null <> null, CHECH PERFORMANCE
            			)
            ),
            to_park_with_affinity_group_in_ready AS (
            	select distinct id
            	from new_portion
            	where
            		affinity_group notnull and affinity notnull
            		and
            			(affinity_group, affinity, 'READY') in (
                        	select affinity_group, affinity, virtual_queue
                            from _____dtf_tasks
            			)
            ),
            to_park_with_affinity_group_in_parked AS (
            	select distinct id
            	from new_portion
            	where
            		affinity_group notnull and affinity notnull
            		and
            			(affinity_group, affinity, 'PARKED') in (
                        	select affinity_group, affinity, virtual_queue
                            from _____dtf_tasks
            			)
            ),
            to_park_by_min_workflow_id AS (
                SELECT id
                FROM new_portion
                WHERE workflow_id != min_workflow_id AND affinity NOTNULL
            ),
            to_park AS (
                SELECT id, 'PARKED'::_____dtf_virtual_queue_type AS target_virtual_queue FROM to_park_without_affinity_group_and_with_affinity_in_ready
                UNION
                SELECT id, 'PARKED'::_____dtf_virtual_queue_type AS target_virtual_queue FROM to_park_without_affinity_group_and_with_affinity_in_parked
                UNION
                SELECT id, 'PARKED'::_____dtf_virtual_queue_type AS target_virtual_queue FROM to_park_with_affinity_group_in_ready
                UNION
                SELECT id, 'PARKED'::_____dtf_virtual_queue_type AS target_virtual_queue FROM to_park_with_affinity_group_in_parked
                UNION
                SELECT id, 'PARKED'::_____dtf_virtual_queue_type AS target_virtual_queue FROM to_park_by_min_workflow_id
            ),
            to_ready as (
                SELECT id, 'READY'::_____dtf_virtual_queue_type AS target_virtual_queue FROM new_portion
                EXCEPT
                SELECT id, 'READY'::_____dtf_virtual_queue_type as target_virtual_queue FROM to_park -- set READY as hack
            ),
            patched_new AS (
                SELECT id, target_virtual_queue from to_park
                UNION all
                SELECT id, target_virtual_queue from to_ready
            )
            UPDATE _____dtf_tasks d
            SET
                virtual_queue = (SELECT target_virtual_queue FROM patched_new WHERE id = d.id),
                version = version + 1
            WHERE d.id IN (SELECT id FROM patched_new)
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
            WITH
            deleted_portion AS (
                SELECT
                    id,
                    affinity_group,
                    affinity,
                    virtual_queue AS target_virtual_queue
                FROM _____dtf_tasks
                WHERE virtual_queue = 'DELETED'
                ORDER BY deleted_at
                LIMIT :limit
            ),
            deleted_afg_af AS (
                SELECT distinct affinity_group, affinity
                FROM deleted_portion
                WHERE affinity NOTNULL
            ),
            is_active_with_affinity_group_and_affinity AS (
                    SELECT distinct affinity_group, affinity
                    FROM deleted_afg_af
                    WHERE
                        affinity_group notnull
                        AND affinity notnull
                        AND (affinity_group, affinity, 'READY') IN (
                            SELECT affinity_group, affinity, virtual_queue
                            FROM _____dtf_tasks
                    )
            ),
            is_active_without_affinity_group_and_with_affinity AS (
                    SELECT distinct affinity
                    FROM deleted_afg_af
                    WHERE
                        affinity_group isnull
                        AND affinity notnull
                        AND (affinity, 'READY') IN ( -- todo: looks like shouldn't work, because null <> null + PERF TEST
                            SELECT affinity, virtual_queue
                            FROM _____dtf_tasks
                            WHERE affinity_group ISNULL
                        )
            ),
            free_with_affinity_group_and_affinity AS (
                SELECT affinity_group, affinity FROM deleted_afg_af WHERE affinity_group NOTNULL AND affinity NOTNULL
                EXCEPT
                SELECT affinity_group, affinity FROM is_active_with_affinity_group_and_affinity
            ),
            free_without_affinity_group_and_with_affinity AS (
                SELECT affinity FROM deleted_afg_af WHERE affinity_group ISNULL AND affinity NOTNULL
                EXCEPT
                SELECT affinity FROM is_active_without_affinity_group_and_with_affinity
            ),
            to_unpark_with_affinity_group_and_affinity_wcd AS (
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
                          FROM free_with_affinity_group_and_affinity
                          WHERE affinity_group NOTNULL AND affinity NOTNULL
                      )
            ),
            to_unpark_without_affinity_group_and_with_affinity_wcd AS (
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
                          SELECT distinct null, affinity, 'PARKED'::_____dtf_virtual_queue_type
                          FROM free_with_affinity_group_and_affinity
                          WHERE affinity_group ISNULL AND affinity NOTNULL
                      )
            ),
            to_unpark_with_affinity_group_and_affinity AS (
                SELECT
                    id,
                    affinity_group,
                    affinity,
                    virtual_queue,
                    'READY'::_____dtf_virtual_queue_type as target_virtual_queue
                FROM _____dtf_tasks
                WHERE (affinity_group, affinity, virtual_queue, workflow_id) IN
                      (
                          SELECT affinity_group, affinity, 'PARKED'::_____dtf_virtual_queue_type, min_workflow_id
                          FROM to_unpark_with_affinity_group_and_affinity_wcd
                          WHERE affinity_group NOTNULL
                      )
            ),
            to_unpark_without_affinity_group_and_with_affinity AS (
                SELECT
                    id,
                    affinity_group,
                    affinity,
                    virtual_queue,
                    'READY'::_____dtf_virtual_queue_type as target_virtual_queue
                FROM _____dtf_tasks
                WHERE (affinity_group, affinity, virtual_queue, workflow_id) IN
                      (
                          SELECT null, affinity, 'PARKED'::_____dtf_virtual_queue_type,  min_workflow_id
                          FROM to_unpark_without_affinity_group_and_with_affinity_wcd
                          WHERE affinity_group ISNULL
                      )
            ),
            to_ready AS (
                SELECT id, affinity_group, affinity, target_virtual_queue FROM to_unpark_with_affinity_group_and_affinity
                UNION ALL
                SELECT id, affinity_group, affinity, target_virtual_queue FROM to_unpark_without_affinity_group_and_with_affinity
            )
            UPDATE _____dtf_tasks dt
            SET
                virtual_queue = 'READY'::_____dtf_virtual_queue_type,
                version = version + 1
            WHERE dt.id IN (SELECT id FROM to_ready)
            RETURNING dt.id, dt.task_name, dt.version, 'READY'::_____dtf_virtual_queue_type as virtual_queue, dt.affinity_group, dt.affinity;
            """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey, _____dtf_tasks_ag_a_vq_idx, _____dtf_tasks_vq_ag_wcdu_idx, _____dtf_tasks_da_vq_idx
    @Override
    public List<ShortTaskEntity> moveParkedToReady(int batchSize) {
        return Lists.newArrayList(namedParameterJdbcTemplate.query(
                        MOVE_PARKED_TO_READY,
                        Map.of(
                                "limit", batchSize
                        ),
                        SHORT_TASK_ROW_MAPPER
                )
        );
    }


    private static final String READY_TO_HARD_DELETE = """
            SELECT id
            FROM _____dtf_tasks
            WHERE virtual_queue = 'DELETED'
            ORDER BY deleted_at
            LIMIT :limit
            """;

    @Override
    public Set<UUID> readyToHardDelete(int batchSize) {
        return Sets.newHashSet(namedParameterJdbcTemplate.queryForList(
                READY_TO_HARD_DELETE,
                        Map.of(
                                "limit", batchSize
                        ),
                        UUID.class
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
                Map.of("virtualQueue", JdbcTools.asString(virtualQueue)),
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
                id = :id
                AND version = :version
            )
            """;

    //SUPPOSED USED INDEXES: _____dtf_tasks_pkey
    @Override
    public void softDelete(TaskEntity taskEntity) {
        int updatedRows = namedParameterJdbcTemplate.update(
                SOFT_DELETE,
                Map.of(
                        "id", taskEntity.getId(),
                        "version", taskEntity.getVersion())
        );
        if (updatedRows == 0) {
            throw new OptimisticLockException(format("Can't update=[%s]", taskEntity), TaskEntity.class);
        }
    }
}
