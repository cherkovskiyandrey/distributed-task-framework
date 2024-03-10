package com.distributed_task_framework.perf_test.persistence.repository.impl;


import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestIntermediateResult;
import com.distributed_task_framework.perf_test.persistence.repository.IntermediateResultExtendedRepository;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class IntermediateResultExtendedRepositoryImpl implements IntermediateResultExtendedRepository {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private static final String SAVE_OR_UPDATE = """
            INSERT INTO dtf_perf_test_intermediate_result(
                affinity_group,
                affinity,
                hierarchy,
                number
            ) VALUES (
                :affinityGroup,
                :affinity,
                :hierarchy,
                :number
            ) ON CONFLICT(affinity_group, affinity, hierarchy) DO UPDATE
            SET
                number = excluded.number
            RETURNING *
            """;

    @Override
    public PerfTestIntermediateResult saveOrUpdate(PerfTestIntermediateResult perfTestIntermediateResult) {
        return namedParameterJdbcTemplate.query(
                SAVE_OR_UPDATE,
                new BeanPropertySqlParameterSource(perfTestIntermediateResult),
                new BeanPropertyRowMapper<>(PerfTestIntermediateResult.class)
        ).get(0);
    }
}
