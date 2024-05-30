package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.persistence.entity.TaskEntity;

import java.util.Collection;
import java.util.List;

public interface InternalTaskCommandService {

    TaskEntity schedule(TaskEntity taskEntity);

    Collection<TaskEntity> scheduleAll(Collection<TaskEntity> taskEntities);

    void reschedule(TaskEntity taskEntity);

    void rescheduleAll(Collection<TaskEntity> taskEntities);

    void forceReschedule(TaskEntity taskEntity);

    void rescheduleAllIgnoreVersion(List<TaskEntity> taskEntities);

    void cancel(TaskEntity taskEntity);

    void cancelAll(Collection<TaskEntity> tasksEntities);

    void finalize(TaskEntity taskEntity);
}
