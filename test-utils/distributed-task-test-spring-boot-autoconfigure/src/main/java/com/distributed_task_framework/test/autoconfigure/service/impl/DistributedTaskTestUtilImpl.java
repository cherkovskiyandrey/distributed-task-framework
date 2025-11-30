package com.distributed_task_framework.test.autoconfigure.service.impl;

import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.repository.DltRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.test.autoconfigure.exception.FailedCancellationException;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class DistributedTaskTestUtilImpl implements DistributedTaskTestUtil {
    public static final int DEFAULT_ATTEMPTS = 10;
    public static final Duration DEFAULT_DURATION = Duration.ofMinutes(1);

    TaskRepository taskRepository;
    DltRepository dltRepository;
    DistributedTaskService distributedTaskService;
    TaskMapper taskMapper;
    WorkerManager workerManager;
    CommonSettings commonSettings;


    @Override
    public void reinitAndWait() throws InterruptedException {
        reinitAndWait(DEFAULT_ATTEMPTS, DEFAULT_DURATION);
    }

    @Override
    public void reinitAndWait(int attemptsToCancel, Duration duration) throws InterruptedException {
        reinitAndWait(attemptsToCancel, duration, List.of());
    }

    @Override
    public void reinitAndWait(int attemptsToCancel,
                              Duration duration,
                              List<TaskDef<?>> excludeList) throws InterruptedException, FailedCancellationException {
        Preconditions.checkArgument(
            attemptsToCancel > 0,
            "attemptsToCancel has to be greater than 0"
        );

        log.info("reinitAndWait(): begin");
        Set<TaskId> activeTaskIds = cancelTasks(attemptsToCancel, excludeList);
        waitForTasksCompletion(activeTaskIds, attemptsToCancel, duration);
        doPostProcessing(excludeList, duration);
        log.info("reinitAndWait(): end");
    }

    private void waitForTasksCompletion(Set<TaskId> activeTaskIds, int attemptsToCancel, Duration duration) throws InterruptedException {
        if (activeTaskIds.isEmpty()) {
            return;
        }

        Exception exception = null;
        for (int attempt = 0; attempt < attemptsToCancel; attempt++) {
            try {
                distributedTaskService.waitCompletionAllWorkflows(activeTaskIds, duration);
                return;
            } catch (TimeoutException e) {
                var workflows = activeTaskIds.stream()
                    .map(TaskId::getWorkflowId)
                    .collect(Collectors.toSet());
                log.warn("Can't wait for any of workflows=[{}] is completed", workflows, e);
                exception = e;
            }
        }

        throw new FailedCancellationException(exception);
    }

    private Set<TaskId> cancelTasks(int attemptsToCancel, List<TaskDef<?>> excludeList) {
        Set<TaskId> activeTaskIds = Sets.newHashSet();
        activeTaskIds.addAll(getAllActiveTasks(excludeList));
        if (activeTaskIds.isEmpty()) {
            return activeTaskIds;
        }

        Exception exception = null;
        for (int attempt = 0; attempt < attemptsToCancel; attempt++) {
            try {
                distributedTaskService.cancelAllWorkflowsByTaskId(Lists.newArrayList(activeTaskIds));
            } catch (Exception e) {
                log.warn("Problem when try to cancel workflows=[{}]", activeTaskIds, e);
                exception = e;
            }

            var restActiveTaskIds = getAllActiveTasks(excludeList);
            activeTaskIds.addAll(restActiveTaskIds);

            if (restActiveTaskIds.isEmpty()) {
                return activeTaskIds;
            }
        }

        throw new FailedCancellationException(exception);
    }

    private List<TaskId> getAllActiveTasks(List<TaskDef<?>> excludeList) {
        var excludedTaskNames = excludeList.stream()
            .map(TaskDef::getTaskName)
            .collect(Collectors.toSet());
        return Lists.newArrayList(taskRepository.findAllNotDeletedAndNotCanceled(excludedTaskNames)).stream()
            .map(task -> taskMapper.map(task, commonSettings.getAppName()))
            .toList();
    }

    private void doPostProcessing(List<TaskDef<?>> excludeList, Duration duration) {
        var excludeTaskNames = excludeList.stream()
            .map(TaskDef::getTaskName)
            .collect(Collectors.toSet());

        await("rest active tasks in worker manager")
            .atMost(duration)
            .pollDelay(Duration.ofMillis(50))
            .until(() -> workerManager.getCurrentActiveTaskIds().stream()
                .allMatch(taskId -> excludeTaskNames.contains(taskId.getTaskName()))
            );

        await("all tasks in deleted queue are completed")
            .atMost(duration)
            .pollDelay(Duration.ofMillis(50))
            .until(() -> Lists.newArrayList(taskRepository.findAll()).stream().filter(Objects::nonNull)
                .allMatch(taskEntity -> excludeTaskNames.contains(taskEntity.getTaskName()))
            );
        dltRepository.deleteAll();
    }
}
