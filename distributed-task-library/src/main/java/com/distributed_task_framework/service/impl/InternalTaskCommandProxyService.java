package com.distributed_task_framework.service.impl;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;

import java.util.Collection;
import java.util.List;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class InternalTaskCommandProxyService implements InternalTaskCommandService {
    List<InternalTaskCommandService> internalTaskCommandServices;

    public InternalTaskCommandProxyService(List<InternalTaskCommandService> internalTaskCommandServices) {
        this.internalTaskCommandServices = internalTaskCommandServices;
    }

    @Override
    public TaskEntity schedule(TaskEntity taskEntity) {
        for (var service : internalTaskCommandServices) {
            taskEntity = service.schedule(taskEntity);
        }
        return taskEntity;
    }

    @Override
    public Collection<TaskEntity> scheduleAll(Collection<TaskEntity> taskEntities) {
        for (var command : internalTaskCommandServices) {
            taskEntities = command.scheduleAll(taskEntities);
        }
        return taskEntities;
    }

    @Override
    public void reschedule(TaskEntity taskEntity) {
        internalTaskCommandServices.forEach(service -> service.reschedule(taskEntity));
    }

    @Override
    public void rescheduleAll(Collection<TaskEntity> taskEntities) {
        internalTaskCommandServices.forEach(service -> service.rescheduleAll(taskEntities));
    }

    @Override
    public void forceReschedule(TaskEntity taskEntity) {
        internalTaskCommandServices.forEach(service -> service.forceReschedule(taskEntity));
    }

    @Override
    public void rescheduleAllIgnoreVersion(List<TaskEntity> taskEntities) {
        internalTaskCommandServices.forEach(service -> service.rescheduleAllIgnoreVersion(taskEntities));
    }

    @Override
    public void cancel(TaskEntity taskEntity) {
        internalTaskCommandServices.forEach(service -> service.cancel(taskEntity));
    }

    @Override
    public void cancelAll(Collection<TaskEntity> tasksEntities) {
        internalTaskCommandServices.forEach(service -> service.cancelAll(tasksEntities));
    }

    @Override
    public void finalize(TaskEntity taskEntity) {
        internalTaskCommandServices.forEach(service -> service.finalize(taskEntity));
    }
}
