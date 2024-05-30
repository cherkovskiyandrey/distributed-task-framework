package com.distributed_task_framework.service.impl;


import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.TaskPopulateAndVerify;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.TaskExtendedRepository;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class VirtualQueueManagerPlannerImplTest extends BaseSpringIntegrationTest {
    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    @Qualifier("taskExtendedRepositoryImpl")
    TaskExtendedRepository taskExtendedRepository;
    @Autowired
    PlatformTransactionManager transactionManager;
    @Autowired
    PartitionTracker partitionTracker;
    @MockBean
    VirtualQueueStatHelper virtualQueueStatHelper;
    @Autowired
    MetricHelper metricHelper;
    @Autowired
    TaskPopulateAndVerify taskPopulateAndVerify;
    VirtualQueueManagerPlannerImpl plannerService;
    ExecutorService executorService;

    @BeforeEach
    public void init() {
        super.init();
        executorService = Executors.newSingleThreadExecutor();
        plannerService = Mockito.spy(new VirtualQueueManagerPlannerImpl(
                commonSettings,
                plannerRepository,
                transactionManager,
                clusterProvider,
                taskRepository,
                partitionTracker,
                taskMapper,
                virtualQueueStatHelper,
                metricHelper
        ));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @SneakyThrows
    @AfterEach
    void destroy() {
        plannerService.shutdown();
        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    void shouldBeInactiveWhenNotAllNodesSupport() {
        //when
        doReturn(false).when(clusterProvider).doAllNodesSupport(eq(Capabilities.VIRTUAL_QUEUE_MANAGER_PLANNER_V1));

        //do & verify
        assertThat(plannerService.hasToBeActive()).isFalse();
    }

    @Test
    void shouldBeActiveWhenAllNodesSupport() {
        //when
        doReturn(true).when(clusterProvider).doAllNodesSupport(eq(Capabilities.VIRTUAL_QUEUE_MANAGER_PLANNER_V1));

        //do & verify
        assertThat(plannerService.hasToBeActive()).isTrue();
    }

    @Test
    void shouldMoveTasks() {
        //when
        setFixedTime();
        List<TaskPopulateAndVerify.PopulationSpec> readyPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 10), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask())
        );
        var alreadyInReady = taskPopulateAndVerify.populate(0, 5, VirtualQueue.READY, readyPopulationSpecs);
        setFixedTime(1_000); //>taskPopulate.populate(5

        //100/10 = 10 tasks for each group are parked
        var parkedTaskEntities = taskPopulateAndVerify.populate(0, 100, VirtualQueue.PARKED, readyPopulationSpecs);
        setFixedTime(2_000); //>taskPopulate.populate(100

        var newTaskEntities = taskPopulateAndVerify.populate(0, 100, VirtualQueue.NEW, readyPopulationSpecs);

        setFixedTime(3_000); //>taskPopulate.populate(100
        taskPopulateAndVerify.populate(0, 100, VirtualQueue.DELETED, readyPopulationSpecs);

        setFixedTime(4_000); //>taskPopulate.populate(100 + 100 + 100 = 300

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        waitForNewVirtualQueueIsEmpty();
        waitForDeletedQueueIsEmpty();

        var newToParkedVerifyCtx = TaskPopulateAndVerify.VerifyVirtualQueueContext.builder()
                .populationSpecRange(Range.closedOpen(0, 10))
                .expectedVirtualQueueByRange(Map.of(
                        Range.closedOpen(0, 10),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.in(VirtualQueue.PARKED)
                ))
                .populationSpecs(readyPopulationSpecs)
                .affectedTaskEntities(newTaskEntities)
                .build();
        taskPopulateAndVerify.verifyVirtualQueue(newToParkedVerifyCtx);

        var parkedToParkedVerifyCtx = TaskPopulateAndVerify.VerifyVirtualQueueContext.builder()
                .populationSpecRange(Range.closedOpen(0, 5))
                .expectedVirtualQueueByRange(Map.of(
                        Range.closedOpen(0, 10),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.in(VirtualQueue.PARKED)
                ))
                .populationSpecs(readyPopulationSpecs)
                .affectedTaskEntities(parkedTaskEntities)
                .build();
        taskPopulateAndVerify.verifyVirtualQueue(parkedToParkedVerifyCtx);

        var parkedToReadyAndParkedVerifyCtx = TaskPopulateAndVerify.VerifyVirtualQueueContext.builder()
                .populationSpecRange(Range.closedOpen(5, 10))
                .expectedVirtualQueueByRange(Map.of(
                        Range.closedOpen(0, 1),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.in(VirtualQueue.READY),

                        Range.closedOpen(1, 10),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.in(VirtualQueue.PARKED)
                ))
                .populationSpecs(readyPopulationSpecs)
                .affectedTaskEntities(parkedTaskEntities)
                .build();
        var groupedTasks = taskPopulateAndVerify.verifyVirtualQueue(parkedToReadyAndParkedVerifyCtx);

        var inReadyTasks = ImmutableList.<TaskEntity>builder()
                .addAll(alreadyInReady)
                .addAll(groupedTasks.get(VirtualQueue.READY))
                .build();
        this.verifyRegisteredPartitionFromTask(inReadyTasks);
    }

    private void waitForDeletedQueueIsEmpty() {
        waitFor(() -> taskRepository.countOfTasksInVirtualQueue(VirtualQueue.DELETED) == 0);
    }

    private void waitForNewVirtualQueueIsEmpty() {
        waitFor(() -> taskRepository.countOfTasksInVirtualQueue(VirtualQueue.NEW) == 0);
    }
}