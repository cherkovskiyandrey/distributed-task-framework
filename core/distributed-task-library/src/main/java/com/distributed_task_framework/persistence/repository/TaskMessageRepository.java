package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.TaskMessageEntity;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TaskMessageRepository extends CrudRepository<TaskMessageEntity, UUID> {

    List<TaskMessageEntity> findAllByJoinTaskIdIn(Collection<UUID> joinTaskIds);

    List<TaskMessageEntity> findAllByTaskToJoinIdAndJoinTaskIdIn(UUID taskToJoinId, Collection<UUID> joinTaskId);

    Optional<TaskMessageEntity> findByTaskToJoinIdAndJoinTaskId(UUID taskToJoinId, UUID joinTaskId);

    @Query("""
            DELETE
            FROM _____dtf_join_task_message_table
            WHERE join_task_id = ANY( (:joinTaskIds)::uuid[] )
            """)
    @Modifying
    void deleteAllByJoinTaskIdIn(@Param("joinTaskIds") String[] joinTaskIds);
}
