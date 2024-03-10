package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.TaskPopulateAndVerify;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class TaskWorkerRepositoryImplTest extends BaseRepositoryTest {
    @Autowired
    @Qualifier("taskWorkerRepositoryImpl")
    TaskWorkerRepository repository;

    @Test
    void shouldGetNextTasks() {
        //when
        var workerId = TaskPopulateAndVerify.getNode(0);
        var foreignWorkerId = TaskPopulateAndVerify.getNode(1);
        var knownPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.allSetWithFixedWorker(2, workerId),
                        Range.closedOpen(1, 2), TaskPopulateAndVerify.GenerationSpec.allSetWithFixedWorker(2, foreignWorkerId)
                )
        );
        var populateNewTasks = taskPopulateAndVerify.populate(0, 10, VirtualQueue.NEW, knownPopulationSpecs);
        var populateReadyTasks = taskPopulateAndVerify.populate(10, 20, VirtualQueue.READY, knownPopulationSpecs);
        var shouldBeIgnored = taskPopulateAndVerify.populate(0, 20, VirtualQueue.READY, knownPopulationSpecs);
        var shouldBeIgnoredIds = shouldBeIgnored.stream()
                .filter(taskEntity -> workerId == taskEntity.getAssignedWorker())
                .map(this::toTaskId)
                .collect(Collectors.toSet());
        //deleted tasks has to be ignored
        taskPopulateAndVerify.populate(0, 20, VirtualQueue.DELETED, knownPopulationSpecs);

        //do
        var actualTasks = repository.getNextTasks(workerId, shouldBeIgnoredIds, 100);

        //verify
        var expectedTasks = Stream.concat(populateNewTasks.stream(), populateReadyTasks.stream())
                .filter(taskEntity -> workerId == taskEntity.getAssignedWorker())
                .filter(taskEntity -> !shouldBeIgnoredIds.contains(toTaskId(taskEntity)))
                .sorted(Comparator.comparing(TaskEntity::getExecutionDateUtc))
                .limit(10)
                .toList();
        Assertions.assertThat(actualTasks).containsExactlyElementsOf(expectedTasks);
    }

    @Test
    void filterCanceled() {
        //when
        var knownPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask(),
                        Range.closedOpen(1, 2), TaskPopulateAndVerify.GenerationSpec.canceled()
                )
        );
        var populateNewTasks = taskPopulateAndVerify.populate(0, 10, VirtualQueue.NEW, knownPopulationSpecs);
        var populateDeletedTasks = taskPopulateAndVerify.populate(0, 10, VirtualQueue.DELETED, knownPopulationSpecs);

        var taskIdToCheck = ImmutableSet.<TaskId>builder()
                .addAll(populateNewTasks.stream().map(this::toTaskId).collect(Collectors.toSet()))
                .addAll(populateDeletedTasks.stream().map(this::toTaskId).collect(Collectors.toSet()))
                .build();

        //do
        Set<TaskId> actualTaskIds = repository.filterCanceled(taskIdToCheck);

        //verify
        var expectedTaskIds = populateNewTasks.stream()
                .filter(TaskEntity::isCanceled)
                .map(this::toTaskId)
                .toList();
        assertThat(actualTaskIds).containsExactlyInAnyOrderElementsOf(expectedTaskIds);
    }
}