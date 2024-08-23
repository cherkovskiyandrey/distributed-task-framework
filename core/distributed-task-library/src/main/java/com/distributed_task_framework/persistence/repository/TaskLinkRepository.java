package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.TaskLinkEntity;
import com.distributed_task_framework.persistence.entity.UUIDEntity;
import com.google.common.annotations.VisibleForTesting;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface TaskLinkRepository extends CrudRepository<TaskLinkEntity, UUID> {

    List<TaskLinkEntity> findAllByTaskToJoinId(UUID taskToJoinId);

    @Query("""
            SELECT DISTINCT(orig.task_to_join_id) AS uuid
            FROM _____dtf_join_task_link_table orig
            WHERE orig.task_to_join_id = ANY( (:taskToJoinIds)::uuid[] )
            """)
    List<UUIDEntity> filterIntermediateTasks(@Param("taskToJoinIds") String[] taskToJoinIds);

    @VisibleForTesting
    List<TaskLinkEntity> findAllByJoinTaskName(String joinTaskName);

    @VisibleForTesting
    List<TaskLinkEntity> findAllByJoinTaskIdIn(List<UUID> joinTaskIds);

    @Query("""
            DELETE
            FROM _____dtf_join_task_link_table
            WHERE task_to_join_id = :taskToJoinId
            """)
    @Modifying
    void deleteAllByTaskToJoinId(@Param("taskToJoinId") UUID taskToJoinId);

    @Query("""
            DELETE
            FROM _____dtf_join_task_link_table
            WHERE join_task_id = ANY( (:joinTaskIds)::uuid[] )
            """)
    @Modifying
    void deleteAllByJoinTaskIdIn(@Param("joinTaskIds") String[] joinTaskIds);

    boolean existsByTaskToJoinId(UUID taskToJoinId);

    @Query("""
            UPDATE _____dtf_join_task_link_table
            SET completed = TRUE
            WHERE task_to_join_id = :taskToJoinId
            """)
    @Modifying
    void markLinksAsCompleted(@Param("taskToJoinId") UUID taskToJoinId);

    @Query("""
            WITH completed_and_parent_detection AS (
                SELECT orig.join_task_id                                              AS join_task_id,
                       orig.completed                                                 AS completed,
                       COUNT(1) OVER (PARTITION BY orig.join_task_id)                 as count_in_group,
                       COUNT(1) OVER (PARTITION BY orig.join_task_id, orig.completed) as completed_in_group
                FROM _____dtf_join_task_link_table orig
                        
            ), completed AS (
                SELECT join_task_id
                FROM completed_and_parent_detection
                WHERE completed = true
                  AND count_in_group = completed_in_group
                        
            )
            SELECT DISTINCT(join_task_id) AS uuid
            FROM completed
            LIMIT :batchSize
            """)
    List<UUIDEntity> getReadyToPlanJoinTasks(@Param("batchSize") int batchSize);

    @Query("""
            WITH RECURSIVE tmp(child_join_task_id, child_join_task_name) AS (
                    SELECT join_task_id, join_task_name
                    FROM _____dtf_join_task_link_table
                    WHERE task_to_join_id = :taskToJoinId
                UNION ALL
                    SELECT orig.join_task_id, orig.join_task_name
                    FROM tmp, _____dtf_join_task_link_table orig
                    WHERE tmp.child_join_task_id = orig.task_to_join_id
            )
            SELECT DISTINCT(child_join_task_id) AS uuid
            FROM tmp
            WHERE child_join_task_name = :joinTaskName;
            """)
    List<UUIDEntity> findAllJoinTasksFromBranchByName(@Param("taskToJoinId") UUID taskToJoinId,
                                                      @Param("joinTaskName") String joinTaskName);

    @Query("""
            SELECT EXISTS (
                WITH RECURSIVE tmp(child_join_task_id, child_join_task_name) AS (
                        SELECT join_task_id, join_task_name
                        FROM _____dtf_join_task_link_table
                        WHERE task_to_join_id = :taskToJoinId
                    UNION ALL
                        SELECT orig.join_task_id, orig.join_task_name
                        FROM tmp, _____dtf_join_task_link_table orig
                        WHERE tmp.child_join_task_id = orig.task_to_join_id
                )
                SELECT child_join_task_id
                FROM tmp
                WHERE child_join_task_id = :joinTaskId
            )
            """)
    boolean checkBranchHasJoinTask(UUID taskToJoinId, UUID joinTaskId);
}
