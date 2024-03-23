package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.JoinTaskExecution;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.repository.PlannerRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PlannerGroups;
import com.distributed_task_framework.service.internal.TaskLinkManager;
import com.distributed_task_framework.settings.CommonSettings;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;


@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class JoinTaskPlannerImpl extends AbstractPlannerImpl {
    public static final String PLANNER_NAME = "join task planner";
    public static final String PLANNER_SHORT_NAME = "jtp";

    TaskLinkManager taskLinkManager;
    TaskRepository taskRepository;
    JoinTaskStatHelper statHelper;
    Clock clock;

    public JoinTaskPlannerImpl(CommonSettings commonSettings,
                               PlannerRepository plannerRepository,
                               PlatformTransactionManager transactionManager,
                               ClusterProvider clusterProvider,
                               TaskLinkManager taskLinkManager,
                               TaskRepository taskRepository,
                               MetricHelper metricHelper,
                               JoinTaskStatHelper statHelper,
                               Clock clock) {
        super(commonSettings, plannerRepository, transactionManager, clusterProvider, metricHelper);
        this.taskLinkManager = taskLinkManager;
        this.taskRepository = taskRepository;
        this.statHelper = statHelper;
        this.clock = clock;
    }

    @Override
    protected String name() {
        return PLANNER_NAME;
    }

    @Override
    protected String shortName() {
        return PLANNER_SHORT_NAME;
    }

    @Override
    protected String groupName() {
        return PlannerGroups.JOIN.getName();
    }

    @Override
    protected boolean inTransaction() {
        return true;
    }

    //TODO: think about optimisation
    @Override
    int processInLoop() {
        List<UUID> joinTaskExecutionIds = taskLinkManager.getReadyToPlanJoinTasks(
                commonSettings.getPlannerSettings().getBatchSize()
        );
        if (joinTaskExecutionIds.isEmpty()) {
            return 0;
        }
        List<TaskEntity> joinTasks = taskRepository.findAll(joinTaskExecutionIds); //opt lock

        List<JoinTaskExecution> joinTasksToPlan = taskLinkManager.prepareJoinTaskToPlan(joinTaskExecutionIds);
        Map<UUID, JoinTaskExecution> joinTaskExecutionsMap = joinTasksToPlan.stream()
                .collect(Collectors.toMap(
                        JoinTaskExecution::getTaskId,
                        Function.identity()
                ));

        LocalDateTime now = LocalDateTime.now(clock);
        joinTasks = joinTasks.stream()
                .map(taskEntity -> taskEntity.toBuilder()
                        .notToPlan(false)
                        .executionDateUtc(now)
                        .joinMessageBytes(joinTaskExecutionsMap.get(taskEntity.getId()).getJoinedMessage())
                        .build())
                .toList();
        Collection<TaskEntity> updatedJoinTasks = taskRepository.saveAll(joinTasks);

        //opt lock check, can throw exception when user cancel/reschedule join task (very rarer case)
        if (updatedJoinTasks.size() != joinTasks.size()) {
            Set<UUID> updatedJoinTaskIds = updatedJoinTasks.stream()
                    .map(TaskEntity::getId)
                    .collect(Collectors.toSet());
            Set<UUID> concurrentChangedTaskIds = Sets.newHashSet(Sets.difference(
                    joinTaskExecutionsMap.keySet(),
                    updatedJoinTaskIds)
            );
            statHelper.updateConcurrentChanged(updatedJoinTasks, concurrentChangedTaskIds);
            throw new OptimisticLockException(
                    "join tasks has been changed concurrently: [%s]".formatted(concurrentChangedTaskIds),
                    TaskEntity.class
            );
        }
        statHelper.updatePlannedTasks(updatedJoinTasks);
        log.info("planJoinTasks(): joinTasksToPlan=[{}]", joinTasksToPlan);

        return joinTaskExecutionIds.size();
    }
}
