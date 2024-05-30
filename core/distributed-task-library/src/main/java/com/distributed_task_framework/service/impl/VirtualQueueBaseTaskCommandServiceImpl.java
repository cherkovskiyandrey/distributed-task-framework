package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.model.WorkerContext;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.service.internal.WorkerContextManager;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class VirtualQueueBaseTaskCommandServiceImpl implements InternalTaskCommandService {
    PartitionTracker partitionTracker;
    ClusterProvider clusterProvider;
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
    public void reschedule(TaskEntity taskEntity) {
        taskRepository.reschedule(routeAsScheduled(taskEntity));
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
    public void rescheduleAllIgnoreVersion(List<TaskEntity> taskEntities) {
        taskEntities = taskEntities.stream()
                .map(this::routeAsScheduled)
                .toList();
        taskRepository.rescheduleAllIgnoreVersion(taskEntities);
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
    public void finalize(TaskEntity taskEntity) {
        if (isVqbActive()) {
            taskRepository.softDelete(taskEntity);
            log.info("finalize(): taskId=[{}] => VirtualQueue.DELETED", taskEntity.getId());
            return;
        }
        taskRepository.hardDelete(taskEntity);
        log.info("finalize(): taskId=[{}] has been deleted", taskEntity.getId());
    }

    private TaskEntity routeAsScheduled(TaskEntity taskEntity) {
        return routeAsScheduled(List.of(taskEntity)).stream().findAny().orElseThrow();
    }

    private Collection<TaskEntity> routeAsScheduled(Collection<TaskEntity> taskEntities) {
        boolean isVqActive = isVqbActive();
        var routedTaskEntities = taskEntities.stream()
                .map(taskEntity -> {
                    var virtualQueue = calcVirtualQueue(taskEntity, isVqActive);
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

    private VirtualQueue calcVirtualQueue(TaskEntity taskEntity, boolean isVqbActive) {
        Optional<WorkerContext> currentContextOpt = workerContextManager.getCurrentContext();
        if (currentContextOpt.isEmpty()) {
            return VirtualQueue.NEW;
        }

        if (!isVqbActive) {
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

    private boolean isVqbActive() {
        return clusterProvider.doAllNodesSupport(Capabilities.VIRTUAL_QUEUE_BASE_FAIR_TASK_PLANNER_V1);
    }
}
