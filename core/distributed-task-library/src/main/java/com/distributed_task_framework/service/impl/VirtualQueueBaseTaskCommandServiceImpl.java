package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.model.WorkerContext;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.TaskIdEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.service.internal.VirtualQueueBaseTaskCommandService;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class VirtualQueueBaseTaskCommandServiceImpl implements VirtualQueueBaseTaskCommandService {
    PartitionTracker partitionTracker;
    TaskRepository taskRepository;
    WorkerContextManager workerContextManager;
    TaskMapper taskMapper;

    @Override
    public TaskEntity schedule(TaskEntity taskEntity) {
        return taskRepository.saveOrUpdate(routeAsScheduled(taskEntity));
    }

    @Override
    public Collection<TaskEntity> scheduleAll(Collection<TaskEntity> taskEntities) {
        return taskRepository.saveAll(routeAsScheduled(taskEntities));
    }

    @Override
    public TaskEntity reschedule(TaskEntity taskEntity) {
        return taskRepository.reschedule(routeAsScheduled(taskEntity));
    }

    @Override
    public void rescheduleAll(Collection<TaskEntity> taskEntities) {
        taskRepository.rescheduleAll(routeAsScheduled(taskEntities));
    }

    @Override
    public void forceReschedule(TaskEntity taskEntity) {
        taskRepository.forceReschedule(routeAsScheduled(taskEntity));
    }

    @Override
    public void forceRescheduleAll(List<TaskEntity> taskEntities) {
        taskEntities = taskEntities.stream()
            .map(this::routeAsScheduled)
            .toList();
        taskRepository.forceRescheduleAll(taskEntities);
    }

    @Override
    public int forceRescheduleAll(TaskDef<?> taskDef, Duration delay, Collection<TaskId> excludes) {
        return taskRepository.forceRescheduleAll(taskDef, delay, excludes);
    }

    @Override
    public void cancel(TaskEntity taskEntity) {
        taskRepository.cancel(taskEntity.getId());
    }

    @Override
    public void cancelAll(Collection<TaskEntity> tasksEntities) {
        var taskIds = tasksEntities.stream()
            .map(TaskEntity::getId)
            .toList();
        taskRepository.cancelAll(taskIds);
    }

    @Override
    public int cancelAll(TaskDef<?> taskDef, Collection<TaskId> excludes) {
        return taskRepository.cancelAll(taskDef, excludes);
    }

    @Override
    public Collection<TaskIdEntity> cancelAll(Collection<UUID> workflows, Collection<TaskId> excludes) {
        return taskRepository.cancelAll(workflows, excludes);
    }

    @Override
    public TaskEntity finalize(TaskEntity taskEntity) {
        var result = taskRepository.softDelete(taskEntity);
        log.info("finalize(): taskId=[{}] => VirtualQueue.DELETED", taskEntity.getId());
        return result;
    }

    @Override
    public void finalizeAll(Collection<TaskEntity> tasksEntities) {
        taskRepository.softDeleteAll(tasksEntities);
        log.info(
            "finalize(): taskIds=[{}] => VirtualQueue.DELETED",
            tasksEntities.stream().map(TaskEntity::getId).toList()
        );
    }

    private TaskEntity routeAsScheduled(TaskEntity taskEntity) {
        return routeAsScheduled(List.of(taskEntity)).stream().findAny().orElseThrow();
    }

    private Collection<TaskEntity> routeAsScheduled(Collection<TaskEntity> taskEntities) {
        var routedTaskEntities = taskEntities.stream()
            .map(taskEntity -> {
                var virtualQueue = calcVirtualQueue(taskEntity);
                log.info("route(): [{}] ==>> [{}]", taskEntity.getId(), virtualQueue);
                return taskEntity.toBuilder()
                    .virtualQueue(virtualQueue)
                    .build();
            })
            .toList();
        var toReadyTaskIds = routedTaskEntities.stream()
            .filter(taskEntity -> VirtualQueue.READY == taskEntity.getVirtualQueue())
            .map(taskMapper::mapToPartition)
            .collect(Collectors.toSet());
        if (!toReadyTaskIds.isEmpty()) {
            partitionTracker.track(toReadyTaskIds);
        }

        return routedTaskEntities;
    }

    private VirtualQueue calcVirtualQueue(TaskEntity taskEntity) {
        Optional<WorkerContext> currentContextOpt = workerContextManager.getCurrentContext();
        if (currentContextOpt.isEmpty()) {
            return VirtualQueue.NEW;
        }

        WorkerContext workerContext = currentContextOpt.get();
        TaskEntity activeTaskEntity = workerContext.getTaskEntity();

        boolean isApplicableToReady = isApplicableToReady(activeTaskEntity, taskEntity);
        if (isApplicableToReady) {
            return VirtualQueue.READY;
        }

        boolean isApplicableToParked = isApplicableToParked(activeTaskEntity, taskEntity);
        return isApplicableToParked ? VirtualQueue.PARKED : VirtualQueue.NEW;
    }

    private boolean isApplicableToReady(TaskEntity activeTaskEntity, TaskEntity secondTaskEntity) {
        return withoutAffinityGroupAndAffinity(secondTaskEntity)
            || withSameAffinityGroupAndAffinity(activeTaskEntity, secondTaskEntity)
            && Objects.equals(
            activeTaskEntity.getWorkflowId(),
            secondTaskEntity.getWorkflowId()
        );
    }

    private boolean withoutAffinityGroupAndAffinity(TaskEntity taskEntity) {
        return taskEntity.getAffinityGroup() == null && taskEntity.getAffinity() == null;
    }

    private boolean isApplicableToParked(TaskEntity firstTaskEntity, TaskEntity secondTaskEntity) {
        return withSameAffinityGroupAndAffinity(firstTaskEntity, secondTaskEntity);
    }

    private boolean withSameAffinityGroupAndAffinity(TaskEntity firstTaskEntity, TaskEntity secondTaskEntity) {
        return Objects.equals(
            firstTaskEntity.getAffinityGroup(),
            secondTaskEntity.getAffinityGroup()
        ) &&
            Objects.equals(
                firstTaskEntity.getAffinity(),
                secondTaskEntity.getAffinity()
            );
    }
}
