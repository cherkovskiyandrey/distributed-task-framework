package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskIdEntity;
import org.springframework.stereotype.Repository;
import com.distributed_task_framework.persistence.entity.TaskEntity;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Repository
public interface TaskCommandRepository {

    /**
     * @param taskEntity
     * @throws OptimisticLockException, UnknownTaskException
     */
    TaskEntity reschedule(TaskEntity taskEntity);

    void rescheduleAll(Collection<TaskEntity> taskEntities);

    @SuppressWarnings("UnusedReturnValue")
    boolean forceReschedule(TaskEntity taskEntity);

    void forceRescheduleAll(List<TaskEntity> tasksToSave);

    int forceRescheduleAll(TaskDef<?> taskDef, Duration delay, Collection<TaskId> excludes);

    boolean cancel(UUID taskId);

    void cancelAll(Collection<UUID> taskIds);

    int cancelAll(TaskDef<?> taskDef, Collection<TaskId> excludes);

    Collection<TaskIdEntity> cancelAll(Collection<UUID> workflows, Collection<TaskId> excludes);
}
