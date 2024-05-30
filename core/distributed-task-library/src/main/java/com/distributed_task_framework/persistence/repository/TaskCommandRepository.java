package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.exception.OptimisticLockException;
import org.springframework.stereotype.Repository;
import com.distributed_task_framework.persistence.entity.TaskEntity;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Repository
public interface TaskCommandRepository {

    /**
     * @param taskEntity
     * @throws OptimisticLockException, UnknownTaskException
     */
    void reschedule(TaskEntity taskEntity);

    void rescheduleAll(Collection<TaskEntity> taskEntities);

    @SuppressWarnings("UnusedReturnValue")
    boolean forceReschedule(TaskEntity taskEntity);

    void rescheduleAllIgnoreVersion(List<TaskEntity> tasksToSave);

    boolean cancel(UUID taskId);

    void cancelAll(Collection<UUID> taskIds);
}
