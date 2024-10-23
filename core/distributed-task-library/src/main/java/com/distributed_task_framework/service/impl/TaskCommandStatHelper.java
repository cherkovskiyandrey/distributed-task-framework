package com.distributed_task_framework.service.impl;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.MetricHelper;

import java.util.Collection;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TaskCommandStatHelper implements InternalTaskCommandService {
    MetricHelper metricHelper;

    @Override
    public TaskEntity schedule(TaskEntity taskEntity) {
        command("schedule", taskEntity);
        return taskEntity;
    }

    @Override
    public Collection<TaskEntity> scheduleAll(Collection<TaskEntity> taskEntities) {
        command("schedule", taskEntities);
        return taskEntities;
    }

    @Override
    public void reschedule(TaskEntity taskEntity) {
        command("reschedule", taskEntity);
    }

    @Override
    public void rescheduleAll(Collection<TaskEntity> taskEntities) {
        command("reschedule", taskEntities);
    }

    @Override
    public void forceReschedule(TaskEntity taskEntity) {
        reschedule(taskEntity);
    }

    @Override
    public void rescheduleAllIgnoreVersion(List<TaskEntity> tasksToSave) {
        tasksToSave.forEach(this::forceReschedule);
    }

    @Override
    public void cancel(TaskEntity taskEntity) {
        command("cancel", taskEntity);
    }

    @Override
    public void cancelAll(Collection<TaskEntity> tasksToDelete) {
        tasksToDelete.forEach(this::cancel);
    }

    @Override
    public void finalize(TaskEntity taskEntity) {
        command("finalize", taskEntity);
    }

    private void command(String command, TaskEntity taskEntity) {
        command(command, List.of(taskEntity));
    }

    private void command(String command, Collection<TaskEntity> taskEntities) {
        taskEntities.forEach(taskEntity ->
                metricHelper.counter(List.of("local", "command", command), List.of(), taskEntity).increment()
        );
    }
}
