package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.RegisteredTaskEntity;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.UUID;

@Repository
public interface RegisteredTaskRepository extends CrudRepository<RegisteredTaskEntity, UUID>, RegisteredTaskExtendedRepository {

    @Modifying
    @Query("DELETE FROM _____dtf_registered_tasks WHERE node_state_id = :nodeStateId")
    void deleteAllByNodeStateId(UUID nodeStateId);

    Collection<RegisteredTaskEntity> findByNodeStateId(UUID nodeStateId);
}
