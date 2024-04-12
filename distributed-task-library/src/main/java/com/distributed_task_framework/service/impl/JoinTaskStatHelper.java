package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.Partition;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Tag;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PlannerGroups;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class JoinTaskStatHelper {
    MetricHelper metricHelper;
    List<Tag> commonTags;
    List<String> plannedCounterName;
    List<String> optLockChangedCounterName;

    public JoinTaskStatHelper(MetricHelper metricHelper) {
        this.metricHelper = metricHelper;
        this.commonTags = List.of(Tag.of("group", PlannerGroups.JOIN.getName()));
        this.plannedCounterName = List.of("planner", "task", "planned");
        this.optLockChangedCounterName = List.of("planner", "optLock", "changed");
    }

    public void updateConcurrentChanged(Collection<TaskEntity> allTasks, Set<UUID> changedTasks) {
        List<TaskEntity> taskEntities = allTasks.stream()
                .filter(joinTaskExecution -> changedTasks.contains(joinTaskExecution.getId()))
                .collect(Collectors.toList());
        updateTaskStat(taskEntities, optLockChangedCounterName);
    }

    public void updatePlannedTasks(Collection<TaskEntity> taskEntities) {
        updateTaskStat(taskEntities, plannedCounterName);
    }

    private void updateTaskStat(Collection<TaskEntity> allTasks, List<String> counterName) {
        Map<Partition, Long> changedTaskEntities = allTasks.stream()
                .collect(Collectors.groupingBy(
                        joinTaskExecution -> new Partition(
                                joinTaskExecution.getAffinityGroup(),
                                joinTaskExecution.getTaskName()
                        ),
                        Collectors.counting()
                ));

        for (var entry : changedTaskEntities.entrySet()) {
            Partition affinityAndTaskName = entry.getKey();
            List<Tag> tags = ImmutableList.<Tag>builder()
                    .addAll(commonTags)
                    .add(metricHelper.buildAffinityGroupTag(affinityAndTaskName.getAffinityGroup()))
                    .add(Tag.of("task_name", affinityAndTaskName.getTaskName()))
                    .add(Tag.of("worker_id", "undefined"))
                    .build();

            metricHelper.counter(counterName, tags).increment(entry.getValue());
        }
    }
}
