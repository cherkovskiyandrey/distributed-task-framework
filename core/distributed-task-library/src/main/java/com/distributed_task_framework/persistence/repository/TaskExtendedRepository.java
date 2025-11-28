package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.persistence.entity.IdVersionEntity;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.TaskIdEntity;
import com.google.common.annotations.VisibleForTesting;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public interface TaskExtendedRepository {
    /**
     * @param taskEntity
     * @return
     * @throws OptimisticLockException
     */
    TaskEntity saveOrUpdate(TaskEntity taskEntity);

    Collection<TaskEntity> saveAll(Collection<TaskEntity> taskEntities);

    void updateAll(Collection<ShortTaskEntity> plannedTasks);

    Optional<TaskEntity> find(UUID taskId);

    @VisibleForTesting
    Collection<TaskIdEntity> findAllNotDeletedAndNotCanceled(Set<String> excludedTaskNames);

    List<TaskEntity> findAll(Collection<UUID> taskIds);

    Collection<TaskEntity> findAllByTaskName(String taskName);

    Collection<TaskEntity> findAllByWorkflowIds(Collection<UUID> workflowIds);

    Collection<TaskEntity> findByName(String taskName, long batchSize);

    Set<UUID> filterExistedWorkflowIds(Set<UUID> workflowIds);

    Set<UUID> filterExistedTaskIds(Set<UUID> requestedTaskIds);

    Collection<TaskIdEntity> findAllTaskId(Collection<UUID> taskIds);

    Collection<IdVersionEntity> deleteByIdVersion(Collection<IdVersionEntity> taskIdVersions);
}
