package com.distributed_task_framework.persistence.repository.jdbc;

import com.distributed_task_framework.persistence.entity.CapabilityEntity;
import com.distributed_task_framework.persistence.entity.NodeStateEntity;
import com.distributed_task_framework.persistence.repository.NodeStateExtendedRepository;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class NodeStateExtendedRepositoryImpl implements NodeStateExtendedRepository {
    NamedParameterJdbcOperations jdbcTemplate;

    public NodeStateExtendedRepositoryImpl(@Qualifier(DTF_JDBC_OPS) NamedParameterJdbcOperations jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    //language=postgresql
    private static final String GET_ALL_WITH_CAPABILITIES = """
        SELECT
            dns.*,
            dc.*
        FROM _____dtf_node_state dns
        LEFT JOIN _____dtf_capabilities dc
        ON dns.node = dc.node_id
        ORDER BY dns.node
        """;

    @Override
    public Map<NodeStateEntity, List<CapabilityEntity>> getAllWithCapabilities() {
        return jdbcTemplate.query(
                GET_ALL_WITH_CAPABILITIES,
                (rs, rowNum) -> Pair.of(
                    NodeStateEntity.NODE_STATE_ENTITY_BEAN_PROPERTY_ROW_MAPPER.mapRow(rs, rowNum),
                    (rs.getString(CapabilityEntity.Fields.id) != null) ?
                        CapabilityEntity.CAPABILITY_ENTITY_BEAN_PROPERTY_ROW_MAPPER.mapRow(rs, rowNum) :
                        null
                )
            ).stream()
            .collect(Collectors.groupingBy(
                Pair::getKey,
                Collectors.mapping(
                    Pair::getValue,
                    Collectors.toList()
                )
            ));
    }
}
