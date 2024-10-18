package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.CapabilityEntity;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Set;
import java.util.UUID;

@Repository
public interface CapabilityRepository extends CrudRepository<CapabilityEntity, UUID>, CapabilityExtendedRepository {
    Set<CapabilityEntity> findByNodeId(UUID nodeId);

    @Query("""
            DELETE FROM _____dtf_capabilities
            WHERE node_id = :nodeId
            """)
    @Modifying
    void deleteAllByNodeId(UUID nodeId);
}
