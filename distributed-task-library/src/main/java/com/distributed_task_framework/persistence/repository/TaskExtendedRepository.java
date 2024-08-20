package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.persistence.entity.IdVersionEntity;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
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

    List<TaskEntity> findAll(Collection<UUID> taskIds);

    Collection<TaskEntity> findAllByTaskName(String taskName);

    Collection<TaskEntity> findByName(String taskName, long batchSize);

    Collection<IdVersionEntity> deleteByIdVersion(Collection<IdVersionEntity> taskIdVersions);
}
