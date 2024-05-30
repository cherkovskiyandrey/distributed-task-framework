package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.NodeStateEntity;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.UUID;

@Repository
public interface NodeStateRepository extends CrudRepository<NodeStateEntity, UUID> {

    @Query("""
            SELECT node
            FROM _____dtf_node_state
            WHERE last_update_date_utc < :obsoleteBoundaryDate
            """
    )
    Collection<UUID> findLostNodes(@Param("obsoleteBoundaryDate") LocalDateTime lostBoundaryDate);
}
