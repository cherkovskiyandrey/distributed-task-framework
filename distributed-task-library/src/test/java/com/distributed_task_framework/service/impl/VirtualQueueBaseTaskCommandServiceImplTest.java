package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.model.WorkerContext;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.doReturn;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class VirtualQueueBaseTaskCommandServiceImplTest extends BaseSpringIntegrationTest {
    private static final String TASK_NAME = "TASK_NAME";

    @Autowired
    VirtualQueueBaseTaskCommandServiceImpl taskCommandService;

    @Test
    void shouldScheduleToNewWhenNotInTaskContext() {
        //when
        var task = createNewTask(null, null);

        //do
        task = taskCommandService.schedule(task);

        //verify
        verifyInQueue(task.getId(), VirtualQueue.NEW);
        verifyPartitionRepositoryIsEmpty();
    }

    @Test
    void shouldScheduleToReadyWhenInTaskContextButCtxTaskInReady() {
        //when
        UUID workflowId = UUID.randomUUID();
        var ctxTask = createNewTask("1", "1", workflowId, VirtualQueue.READY);
        var task = createNewTask("1", "1", workflowId);

        inWorkerContext(ctxTask);

        //do
        task = taskCommandService.schedule(task);

        //verify
        verifyInQueue(task.getId(), VirtualQueue.READY);
        verifyRegisteredPartition("1", TASK_NAME);
    }

    @Test
    void shouldScheduleToParkedWhenInTaskContextWorkflowIdIsOther() {
        //when
        var ctxTask = createNewTask("1", "1", UUID.randomUUID(), VirtualQueue.READY);
        var task = createNewTask("1", "1", UUID.randomUUID());

        inWorkerContext(ctxTask);

        //do
        task = taskCommandService.schedule(task);

        //verify
        verifyInQueue(task.getId(), VirtualQueue.PARKED);
        verifyPartitionRepositoryIsEmpty();
    }

    @Test
    void shouldScheduleToNewWhenInTaskContextAffinityIsOther() {
        //when
        UUID workflowId = UUID.randomUUID();
        var ctxTask = createNewTask("1", "1", workflowId, VirtualQueue.READY);
        var task = createNewTask("1", "2", workflowId);

        inWorkerContext(ctxTask);

        //do
        task = taskCommandService.schedule(task);

        //verify
        verifyInQueue(task.getId(), VirtualQueue.NEW);
        verifyPartitionRepositoryIsEmpty();
    }

    @Test
    void shouldScheduleToActiveWhenInTaskContextAffinityGroupAndAffinityIsNull() {
        //when
        UUID workflowId = UUID.randomUUID();
        var ctxTask = createNewTask("1", "1", workflowId, VirtualQueue.READY);
        var task = createNewTask(null, null, workflowId);

        inWorkerContext(ctxTask);

        //do
        task = taskCommandService.schedule(task);

        //verify
        verifyInQueue(task.getId(), VirtualQueue.READY);
        verifyRegisteredPartition(null, TASK_NAME);
    }

    @Test
    void shouldScheduleAllToNewWhenNotInTaskContext() {
        //when
        var task1 = createNewTask(null, null);
        var task2 = createNewTask(null, null);

        //do
        var tasks = taskCommandService.scheduleAll(List.of(task1, task2));

        //verify
        verifyInQueue(toIds(tasks), VirtualQueue.NEW);
        verifyPartitionRepositoryIsEmpty();
    }

    @Test
    void shouldScheduleAllToReadyWhenInTaskContextButCtxTaskInReady() {
        //when
        UUID workflowId = UUID.randomUUID();
        var ctxTask = createNewTask("1", "1", workflowId, VirtualQueue.READY);
        var task1 = createNewTask("1", "1", workflowId);
        var task2 = createNewTask("1", "1", workflowId);

        inWorkerContext(ctxTask);

        //do
        var tasks = taskCommandService.scheduleAll(List.of(task1, task2));

        //verify
        verifyInQueue(toIds(tasks), VirtualQueue.READY);
        verifyRegisteredPartition("1", TASK_NAME);
    }

    @Test
    void shouldScheduleAllToParkedWhenInTaskContextWorkflowIdIsOther() {
        //when
        var ctxTask = createNewTask("1", "1", UUID.randomUUID(), VirtualQueue.READY);
        var task1 = createNewTask("1", "1", UUID.randomUUID());
        var task2 = createNewTask("1", "1", UUID.randomUUID());

        inWorkerContext(ctxTask);

        //do
        var tasks = taskCommandService.scheduleAll(List.of(task1, task2));

        //verify
        verifyInQueue(toIds(tasks), VirtualQueue.PARKED);
        verifyPartitionRepositoryIsEmpty();
    }

    @Test
    void shouldScheduleAllToNewWhenInTaskContextAffinityIsOther() {
        //when
        UUID workflowId = UUID.randomUUID();
        var ctxTask = createNewTask("1", "1", workflowId, VirtualQueue.READY);
        var task1 = createNewTask("1", "2", workflowId);
        var task2 = createNewTask("1", "2", workflowId);

        inWorkerContext(ctxTask);

        //do
        var tasks = taskCommandService.scheduleAll(List.of(task1, task2));

        //verify
        verifyInQueue(toIds(tasks), VirtualQueue.NEW);
        verifyPartitionRepositoryIsEmpty();
    }

    @Test
    void shouldScheduleAllToActiveWhenInTaskContextAffinityGroupAndAffinityIsNull() {
        //when
        UUID workflowId = UUID.randomUUID();
        var ctxTask = createNewTask("1", "1", workflowId, VirtualQueue.READY);
        var task1 = createNewTask(null, null, workflowId);
        var task2 = createNewTask(null, null, workflowId);

        inWorkerContext(ctxTask);

        //do
        var tasks = taskCommandService.scheduleAll(List.of(task1, task2));

        //verify
        verifyInQueue(toIds(tasks), VirtualQueue.READY);
        verifyRegisteredPartition(null, TASK_NAME);
    }

    @Test
    void shouldSoftDeleteWhenFinalize() {
        //when
        var task = createNewTask("1", "2", UUID.randomUUID(), VirtualQueue.NEW);
        task = taskRepository.saveOrUpdate(task);

        //do
        taskCommandService.finalize(task);

        //verify
        verifyInQueue(task.getId(), VirtualQueue.DELETED);
        verifyPartitionRepositoryIsEmpty();
    }

    //todo: other methods from taskRepository

    private void inWorkerContext(TaskEntity taskEntity) {
        doReturn(Optional.of(
            WorkerContext.builder()
                .taskEntity(taskEntity)
                .build()
        )).when(workerContextManager).getCurrentContext();
    }

    private TaskEntity createNewTask(String afg, String affinity) {
        return createNewTask(afg, affinity, UUID.randomUUID(), VirtualQueue.NEW);
    }

    private TaskEntity createNewTask(String afg, String affinity, UUID workflowId) {
        return createNewTask(afg, affinity, workflowId, VirtualQueue.NEW);
    }

    private TaskEntity createNewTask(String afg, String affinity, UUID workflowId, VirtualQueue virtualQueue) {
        return TaskEntity.builder()
            .taskName(TASK_NAME)
            .affinityGroup(afg)
            .affinity(affinity)
            .workflowId(workflowId)
            .virtualQueue(virtualQueue)
            .executionDateUtc(LocalDateTime.now(clock))
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .createdDateUtc(LocalDateTime.now(clock))
            .build();
    }

    private void verifyInQueue(UUID taskId, VirtualQueue virtualQueue) {
        verifyInQueue(List.of(taskId), virtualQueue);
    }

    private void verifyInQueue(Collection<UUID> taskIds, VirtualQueue virtualQueue) {
        Assertions.assertThat(taskRepository.findAllById(taskIds))
            .hasSize(taskIds.size())
            .allMatch(taskEntity -> virtualQueue == taskEntity.getVirtualQueue());
    }

    private void verifyPartitionRepositoryIsEmpty() {
        Assertions.assertThat(partitionRepository.findAll()).isEmpty();
    }

    private void verifyRegisteredPartition(@Nullable String affinityGroup, String taskName) {
        var expectedPartition = Partition.builder()
            .affinityGroup(affinityGroup)
            .taskName(taskName)
            .build();
        Assertions.assertThat(partitionRepository.findAll())
            .map(partitionMapper::fromEntity)
            .singleElement()
            .isEqualTo(expectedPartition);
    }
}