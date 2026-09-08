package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.persistence.entity.PartitionEntity;
import com.distributed_task_framework.persistence.repository.PartitionExtendedRepository;
import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import com.distributed_task_framework.utils.JdbcTools;
import com.distributed_task_framework.utils.PgComparatorUtils;
import com.distributed_task_framework.utils.SqlParameters;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class PartitionExtendedRepositoryImpl implements PartitionExtendedRepository {
    NamedParameterJdbcOperations namedParameterJdbcTemplate;

    public PartitionExtendedRepositoryImpl(DtfJdbcInfrastructure dtfJdbcInfrastructure) {
        this.namedParameterJdbcTemplate = dtfJdbcInfrastructure.getNamedParameterJdbcOperations();
    }

    private static final String SAVE_OR_UPDATE_BATCH = """
        INSERT INTO _____dtf_partitions (
            id,
            affinity_group,
            task_name,
            time_bucket
        ) VALUES (
            :id::uuid,
            :affinityGroup,
            :taskName,
            :timeBucket
        )
        """;

    @Override
    public Collection<PartitionEntity> saveAsNew(Collection<PartitionEntity> taskNameEntities) {
        var batchArguments = taskNameEntities.stream()
            .map(this::prepareToSave)
            .map(this::toParameterSource)
            .toArray(SqlParameterSource[]::new);
        int[] affectedRows = namedParameterJdbcTemplate.batchUpdate(SAVE_OR_UPDATE_BATCH, batchArguments);
        return Sets.newHashSet(JdbcTools.filterAffected(Lists.newArrayList(taskNameEntities), affectedRows));
    }

    private PartitionEntity prepareToSave(PartitionEntity partitionEntity) {
        partitionEntity.setId(UUID.randomUUID());
        return partitionEntity;
    }

    private SqlParameterSource toParameterSource(PartitionEntity partitionEntity) {
        MapSqlParameterSource result = new MapSqlParameterSource();
        result.addValue(PartitionEntity.Fields.id, JdbcTools.asNullableString(partitionEntity.getId()), Types.VARCHAR);
        result.addValue(PartitionEntity.Fields.affinityGroup, partitionEntity.getAffinityGroup(), Types.VARCHAR);
        result.addValue(PartitionEntity.Fields.taskName, partitionEntity.getTaskName(), Types.VARCHAR);
        result.addValue(PartitionEntity.Fields.timeBucket, partitionEntity.getTimeBucket(), Types.INTEGER);
        return result;
    }


    private static final String FILTER_EXISTED = """
        WITH filter AS (
            SELECT DISTINCT affinity_group, task_name, time_bucket
            FROM UNNEST(:affinityGroup::varchar[], :taskName::varchar[], :timeBucket::int[])
            AS tmp(affinity_group, task_name, time_bucket)
        )
        SELECT f.affinity_group, f.task_name, f.time_bucket
        FROM filter f
        WHERE f.affinity_group IS NOT NULL
            AND EXISTS (
                SELECT 1
                FROM _____dtf_partitions p
                WHERE p.affinity_group = f.affinity_group
                AND p.task_name = f.task_name
                AND p.time_bucket = f.time_bucket
            )
        UNION ALL
        SELECT f.affinity_group, f.task_name, f.time_bucket
        FROM filter f
        WHERE f.affinity_group IS NULL
            AND EXISTS (
                SELECT 1
                FROM _____dtf_partitions p
                WHERE p.affinity_group IS NULL
                AND p.task_name = f.task_name
                AND p.time_bucket = f.time_bucket
            )
        """;

    @Override
    public Collection<PartitionEntity> filterExisted(Collection<PartitionEntity> entities) {
        List<String> affinityGroups = entities.stream().map(PartitionEntity::getAffinityGroup).toList();
        List<String> taskNames = entities.stream().map(PartitionEntity::getTaskName).toList();
        List<Long> timeBuckets = entities.stream().map(PartitionEntity::getTimeBucket).toList();
        return namedParameterJdbcTemplate.query(
            FILTER_EXISTED,
            SqlParameters.of(
                PartitionEntity.Fields.affinityGroup, JdbcTools.toArray(affinityGroups), Types.ARRAY,
                PartitionEntity.Fields.taskName, JdbcTools.toArray(taskNames), Types.ARRAY,
                PartitionEntity.Fields.timeBucket, JdbcTools.toLongArray(timeBuckets), Types.ARRAY
            ),
            PartitionEntity.PARTITION_ROW_MAPPER
        );
    }


    private static final String SELECT_ALL_BEFORE_TIME_WINDOW = """
        SELECT *
        FROM _____dtf_partitions
        WHERE time_bucket <= :maxTimeBucket
        """;

    @Override
    public Collection<PartitionEntity> findAllBeforeOrIn(Long maxTimeBucket) {
        return namedParameterJdbcTemplate.query(
            SELECT_ALL_BEFORE_TIME_WINDOW,
            SqlParameters.of("maxTimeBucket", maxTimeBucket, Types.BIGINT),
            PartitionEntity.PARTITION_ROW_MAPPER
        );
    }


    private static final String COMPACT_IN_TIME_BUCKET = """
        WITH unique_ids AS (
            SELECT DISTINCT on (affinity_group, task_name, time_bucket)
                id
            FROM _____dtf_partitions
            WHERE time_bucket = :timeBucket
            ORDER BY affinity_group, task_name, time_bucket, id
        ),
        to_delete AS (
            SELECT id
            FROM _____dtf_partitions
            WHERE time_bucket = :timeBucket
              AND id NOT IN (SELECT id FROM unique_ids)
            ORDER BY id
            FOR UPDATE
        )
        DELETE FROM _____dtf_partitions
        WHERE id IN (SELECT id FROM to_delete)
        """;

    @Override
    public void compactInTimeWindow(Long timeBucket) {
        namedParameterJdbcTemplate.update(
            COMPACT_IN_TIME_BUCKET,
            SqlParameters.of(PartitionEntity.Fields.timeBucket, timeBucket, Types.BIGINT)
        );
    }

    private static final String DELETE_BY_IDS = """
        DELETE FROM _____dtf_partitions
        WHERE id = ANY( (:ids)::uuid[] )
        """;

    @Override
    public void deleteBatch(Collection<PartitionEntity> toRemove) {
        Collection<UUID> ids = toRemove.stream().map(PartitionEntity::getId).toList();
        ids = PgComparatorUtils.sortTaskIds(ids);
        namedParameterJdbcTemplate.update(
            DELETE_BY_IDS,
            SqlParameters.of(
                "ids", JdbcTools.UUIDsToStringArray(ids), Types.ARRAY
            )
        );
    }
}
