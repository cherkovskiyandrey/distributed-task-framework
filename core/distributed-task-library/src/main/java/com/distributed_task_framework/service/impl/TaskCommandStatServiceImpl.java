package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.TaskIdEntity;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.TaskCommandStatService;
import io.micrometer.core.instrument.Tag;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;

import static com.distributed_task_framework.service.internal.MetricHelper.TASK_NAME_TAG_NAME;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TaskCommandStatServiceImpl implements TaskCommandStatService {
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
    public void forceRescheduleAll(List<TaskEntity> tasksToSave) {
        tasksToSave.forEach(this::forceReschedule);
    }

    @Override
    public void forceRescheduleAll(TaskDef<?> taskDef, int number) {
        metricHelper.counter(
            List.of("local", "command", "reschedule"),
            List.of(Tag.of(TASK_NAME_TAG_NAME, taskDef.getTaskName()))
        ).increment(number);
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
    public void cancelAllTaskIds(Collection<TaskIdEntity> taskIdEntities) {
        taskIdEntities.forEach(taskIdEntity ->
            metricHelper.counter(
                List.of("local", "command", "cancel"),
                List.of(Tag.of(TASK_NAME_TAG_NAME, taskIdEntity.getTaskName()))
            )
        );
    }

    @Override
    public void cancelAll(TaskDef<?> taskDef, int number) {
        metricHelper.counter(
            List.of("local", "command", "cancel"),
            List.of(Tag.of(TASK_NAME_TAG_NAME, taskDef.getTaskName()))
        ).increment(number);
    }

    @Override
    public void finalize(TaskEntity taskEntity) {
        command("finalize", taskEntity);
    }

    @Override
    public void finalizeAll(Collection<TaskEntity> tasksEntities) {
        command("finalize", tasksEntities);
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
