package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.TaskIdEntity;

import java.util.Collection;
import java.util.List;

public interface TaskCommandStatService {

    TaskEntity schedule(TaskEntity taskEntity);

    Collection<TaskEntity> scheduleAll(Collection<TaskEntity> taskEntities);

    void reschedule(TaskEntity taskEntity);

    void rescheduleAll(Collection<TaskEntity> taskEntities);

    void forceReschedule(TaskEntity taskEntity);

    void forceRescheduleAll(List<TaskEntity> taskEntities);

    void forceRescheduleAll(TaskDef<?> taskDef, int number);

    void cancel(TaskEntity taskEntity);

    void cancelAll(Collection<TaskEntity> tasksEntities);

    void cancelAllTaskIds(Collection<TaskIdEntity> taskIdEntities);

    void cancelAll(TaskDef<?> taskDef, int number);

    void finalize(TaskEntity taskEntity);

    void finalizeAll(Collection<TaskEntity> tasksEntities);
}
