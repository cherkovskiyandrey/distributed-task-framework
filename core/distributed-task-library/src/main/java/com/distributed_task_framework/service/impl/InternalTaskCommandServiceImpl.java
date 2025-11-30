package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.TaskCommandStatService;
import com.distributed_task_framework.service.internal.VirtualQueueBaseTaskCommandService;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class InternalTaskCommandServiceImpl implements InternalTaskCommandService {
    VirtualQueueBaseTaskCommandService virtualQueueBaseTaskCommandService;
    TaskCommandStatService taskCommandStatService;

    @Override
    public TaskEntity schedule(TaskEntity taskEntity) {
        return taskCommandStatService.schedule(
            virtualQueueBaseTaskCommandService.schedule(taskEntity)
        );
    }

    @Override
    public Collection<TaskEntity> scheduleAll(Collection<TaskEntity> taskEntities) {
        return taskCommandStatService.scheduleAll(
            virtualQueueBaseTaskCommandService.scheduleAll(taskEntities)
        );
    }

    @Override
    public TaskEntity reschedule(TaskEntity taskEntity) {
        var result = virtualQueueBaseTaskCommandService.reschedule(taskEntity);
        taskCommandStatService.reschedule(taskEntity);
        return result;
    }

    @Override
    public void rescheduleAll(Collection<TaskEntity> taskEntities) {
        virtualQueueBaseTaskCommandService.rescheduleAll(taskEntities);
        taskCommandStatService.rescheduleAll(taskEntities);
    }

    @Override
    public void forceReschedule(TaskEntity taskEntity) {
        virtualQueueBaseTaskCommandService.forceReschedule(taskEntity);
        taskCommandStatService.forceReschedule(taskEntity);
    }

    @Override
    public void forceRescheduleAll(List<TaskEntity> taskEntities) {
        virtualQueueBaseTaskCommandService.forceRescheduleAll(taskEntities);
        taskCommandStatService.forceRescheduleAll(taskEntities);
    }

    @Override
    public int forceRescheduleAll(TaskDef<?> taskDef, Duration delay, Collection<TaskId> excludes) {
        int number = virtualQueueBaseTaskCommandService.forceRescheduleAll(taskDef, delay, excludes);
        taskCommandStatService.forceRescheduleAll(taskDef, number);
        return number;
    }

    @Override
    public void cancel(TaskEntity taskEntity) {
        virtualQueueBaseTaskCommandService.cancel(taskEntity);
        taskCommandStatService.cancel(taskEntity);
    }

    @Override
    public void cancelAll(Collection<TaskEntity> tasksEntities) {
        virtualQueueBaseTaskCommandService.cancelAll(tasksEntities);
        taskCommandStatService.cancelAll(tasksEntities);
    }

    @Override
    public int cancelAll(TaskDef<?> taskDef, Collection<TaskId> excludes) {
        int number = virtualQueueBaseTaskCommandService.cancelAll(taskDef, excludes);
        taskCommandStatService.cancelAll(taskDef, number);
        return number;
    }

    @Override
    public int cancelAll(Collection<UUID> workflows, Collection<TaskId> excludes) {
        var canceledTaskIds = virtualQueueBaseTaskCommandService.cancelAll(workflows, excludes);
        taskCommandStatService.cancelAllTaskIds(canceledTaskIds);
        return canceledTaskIds.size();
    }

    @Override
    public TaskEntity finalize(TaskEntity taskEntity) {
        var result = virtualQueueBaseTaskCommandService.finalize(taskEntity);
        taskCommandStatService.finalize(taskEntity);
        return result;
    }

    @Override
    public void finalizeAll(Collection<TaskEntity> tasksEntities) {
        virtualQueueBaseTaskCommandService.finalizeAll(tasksEntities);
        taskCommandStatService.finalizeAll(tasksEntities);
    }
}
