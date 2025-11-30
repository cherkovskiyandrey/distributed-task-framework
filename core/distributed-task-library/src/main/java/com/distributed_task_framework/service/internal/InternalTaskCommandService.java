package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface InternalTaskCommandService {

    TaskEntity schedule(TaskEntity taskEntity);

    @SuppressWarnings("UnusedReturnValue")
    Collection<TaskEntity> scheduleAll(Collection<TaskEntity> taskEntities);

    TaskEntity reschedule(TaskEntity taskEntity);

    void rescheduleAll(Collection<TaskEntity> taskEntities);

    void forceReschedule(TaskEntity taskEntity);

    void forceRescheduleAll(List<TaskEntity> taskEntities);

    int forceRescheduleAll(TaskDef<?> taskDef, Duration delay, Collection<TaskId> excludes);

    void cancel(TaskEntity taskEntity);

    void cancelAll(Collection<TaskEntity> tasksEntities);

    int cancelAll(TaskDef<?> taskDef, Collection<TaskId> excludes);

    int cancelAll(Collection<UUID> workflows, Collection<TaskId> excludes);

    TaskEntity finalize(TaskEntity taskEntity);

    void finalizeAll(Collection<TaskEntity> tasksEntities);
}
