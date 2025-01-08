package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.model.NodeLoading;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.TaskPopulateAndVerify;
import com.distributed_task_framework.persistence.entity.PartitionEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.TaskSettings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.util.Pair;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.oneWithAffinityGroupAndTaskNameAndCreatedDate;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.oneWithCreatedDateAndWithoutAffinity;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.oneWithWorkerAndCreatedDateAndWithoutAffinity;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.withCreatedDateAndWithoutAffinity;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.withWorkerAndWithoutAffinity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

//todo: tests
@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class VirtualQueueBaseFairTaskPlannerImplTest extends BaseSpringIntegrationTest {
    private static final String TASK_0 = TaskPopulateAndVerify.getTaskName(0);
    private static final String TASK_1 = TaskPopulateAndVerify.getTaskName(1);
    private static final String TASK_2 = TaskPopulateAndVerify.getTaskName(2);

    private static final UUID NODE0 = TaskPopulateAndVerify.getNode(0);
    private static final UUID NODE1 = TaskPopulateAndVerify.getNode(1);
    private static final UUID NODE2 = TaskPopulateAndVerify.getNode(2);

    private static final String AFG_1 = "AFG_1";
    private static final String AFG_2 = "AFG_2";

    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    PlatformTransactionManager transactionManager;
    @Autowired
    PartitionTracker partitionTracker;
    @Autowired
    VirtualQueueStatHelper virtualQueueStatHelper;
    @Autowired
    MetricHelper metricHelper;
    @Autowired
    TaskPopulateAndVerify taskPopulateAndVerify;

    VirtualQueueBaseFairTaskPlannerImpl plannerService;
    ExecutorService executorService;

    @BeforeEach
    public void init() {
        super.init();
        executorService = Executors.newSingleThreadExecutor();
        plannerService = Mockito.spy(new VirtualQueueBaseFairTaskPlannerImpl(
            commonSettings,
            plannerRepository,
            transactionManager,
            clusterProvider,
            taskRepository,
            partitionTracker,
            taskRegistryService,
            new TaskRouter(),
            virtualQueueStatHelper,
            clock,
            metricHelper
        ));
        mockNodeCpuLoading(0.1);
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
    void shouldBeActiveWhenAllNodesSupport() {
        //when
        doReturn(true)
            .when(clusterProvider)
            .doAllNodesSupport(eq(Capabilities.VIRTUAL_QUEUE_BASE_FAIR_TASK_PLANNER_V1));
        doReturn(true).when(clusterProvider).isNodeRegistered();

        //do & verify
        assertThat(plannerService.hasToBeActive()).isTrue();
    }

    @Test
    void shouldNotBeActiveWhenNotAllNodesSupport() {
        //when
        doReturn(false).when(clusterProvider).doAllNodesSupport(eq(Capabilities.VIRTUAL_QUEUE_BASE_FAIR_TASK_PLANNER_V1));

        //do & verify
        assertThat(plannerService.hasToBeActive()).isFalse();
    }

    @Test
    void shouldPlanUnplannedTasks() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef(TASK_0, String.class);
        waitForNodeIsRegistered(taskDef);
        registerPartition(TASK_0);

        log.info("shouldPlanUnplannedTasks(): begging of population");
        var readyPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 1), oneWithCreatedDateAndWithoutAffinity(LocalDateTime.now(clock)))
        );
        var unplannedTasks = taskPopulateAndVerify.populate(0, 10, VirtualQueue.READY, readyPopulationSpecs);

        var delayedPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 1), oneWithCreatedDateAndWithoutAffinity(LocalDateTime.now(clock).plusHours(1)))
        );
        var delayedTasks = taskPopulateAndVerify.populate(0, 10, VirtualQueue.READY, delayedPopulationSpecs);
        log.info("shouldPlanUnplannedTasks(): finished population");

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        verifyAllHaveCurrentNodeWorker(unplannedTasks);
        verifyAllHaveNotWorker(delayedTasks);
    }

    @Test
    void shouldPlanOrphanedTasks() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef(TASK_0, String.class);
        waitForNodeIsRegistered(taskDef);
        registerPartition(TASK_0);

        log.info("shouldPlanOrphanedTasks(): begging of population");
        UUID lostNodeId = UUID.randomUUID();
        var unplannedPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 1), oneWithWorkerAndCreatedDateAndWithoutAffinity(lostNodeId, LocalDateTime.now(clock)))
        );
        var unplannedTasks = taskPopulateAndVerify.populate(0, 20, VirtualQueue.READY, unplannedPopulationSpecs);
        log.info("shouldPlanOrphanedTasks(): finished population");

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        verifyAllHaveCurrentNodeWorker(unplannedTasks);
    }

    @Test
    void shouldIgnoreUnknownTaskDefWhenPlan() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef(TASK_0, String.class);
        TaskDef<String> unknownTaskDef = TaskDef.privateTaskDef(TASK_1, String.class);
        waitForNodeIsRegistered(taskDef, unknownTaskDef);
        registerPartition(List.of(TASK_0, TASK_1));

        var unplannedPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 2), withCreatedDateAndWithoutAffinity(2, LocalDateTime.now(clock)))
        );
        var unplannedTasks = taskPopulateAndVerify.populate(0, 20, VirtualQueue.READY, unplannedPopulationSpecs);

        waitForTasksUnregistered(unknownTaskDef);

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        Collection<TaskEntity> haveToPlannedTasks = unplannedTasks.stream()
            .filter(taskEntity -> TASK_0.equals(taskEntity.getTaskName()))
            .toList();
        Collection<TaskEntity> unknownTasks = unplannedTasks.stream()
            .filter(taskEntity -> TASK_1.equals(taskEntity.getTaskName()))
            .toList();

        verifyAllHaveCurrentNodeWorker(haveToPlannedTasks);
        verifyAllHaveNotWorker(unknownTasks);
    }

    @Test
    void shouldNotPlanGreaterThenMaxParallelInCluster() {
        //when
        TaskDef<String> taskDef1 = TaskDef.privateTaskDef(TASK_0, String.class);
        TaskDef<String> taskDef2 = TaskDef.privateTaskDef(TASK_1, String.class);
        waitForNodeIsRegistered(List.of(
                Pair.of(taskDef1, defaultTaskSettings.toBuilder().maxParallelInCluster(2).build()),
                Pair.of(taskDef2, defaultTaskSettings.toBuilder().maxParallelInCluster(3).build())
            )
        );
        registerPartition(List.of(TASK_0, TASK_1));

        log.info("shouldApplyRestrictionsWhenPlan(): begging of population");
        var alreadyPlannedPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 2), withWorkerAndWithoutAffinity(clusterProvider.nodeId(), 2))
        );
        taskPopulateAndVerify.populate(0, 2, VirtualQueue.READY, alreadyPlannedPopulationSpecs);

        var unplannedPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 2), withCreatedDateAndWithoutAffinity(2, LocalDateTime.now(clock)))
        );
        taskPopulateAndVerify.populate(0, 200, VirtualQueue.READY, unplannedPopulationSpecs);
        log.info("shouldApplyRestrictionsWhenPlan(): finished population");
        AtomicInteger invocationVerifier = spyProcessInLoopInvocation();

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        waitFor(() -> invocationVerifier.get() > 1);
        Assertions.assertThat(filterAssigned(taskRepository.findAllByTaskName(taskDef1.getTaskName())))
            .hasSize(2);
        Assertions.assertThat(filterAssigned(taskRepository.findAllByTaskName(taskDef2.getTaskName())))
            .hasSize(3);
    }

    @Test
    void shouldNotPlanGreaterThenMaxParallelInClusterWhenAlreadyMaxAndUnlimited() {
        //when
        TaskDef<String> taskDef1 = TaskDef.privateTaskDef(TASK_0, String.class);
        TaskDef<String> taskDef2 = TaskDef.privateTaskDef(TASK_1, String.class);
        waitForNodeIsRegistered(List.of(
                Pair.of(taskDef1, defaultTaskSettings.toBuilder().maxParallelInCluster(2).build()),
                Pair.of(taskDef2, defaultTaskSettings.toBuilder().build())
            )
        );
        registerPartition(List.of(TASK_0, TASK_1));

        log.info("shouldApplyRestrictionsWhenPlan(): begging of population");
        var alreadyPlannedPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 2), withWorkerAndWithoutAffinity(clusterProvider.nodeId(), 2))
        );
        taskPopulateAndVerify.populate(0, 3, VirtualQueue.READY, alreadyPlannedPopulationSpecs);

        var unplannedPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 2), withCreatedDateAndWithoutAffinity(2, LocalDateTime.now(clock)))
        );
        taskPopulateAndVerify.populate(0, 200, VirtualQueue.READY, unplannedPopulationSpecs);
        log.info("shouldApplyRestrictionsWhenPlan(): finished population");

        AtomicInteger invocationVerifier = spyProcessInLoopInvocation();

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        waitFor(() -> invocationVerifier.get() > 1);
        Assertions.assertThat(filterAssigned(taskRepository.findAllByTaskName(taskDef1.getTaskName())))
            .hasSize(2);
        Assertions.assertThat(filterAssigned(taskRepository.findAllByTaskName(taskDef2.getTaskName())))
            .hasSize(15); //was 1 + free capacity=7 * planFactor = 14
    }

    @Test
    void shouldNotPlanWhenNotToPlanIsSet() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef(TASK_0, String.class);
        waitForNodeIsRegistered(taskDef);
        registerPartition(TASK_0);

        log.info("shouldNotPlanWhenNotToPlanIsSet(): begging of population");
        TaskEntity unplannedTask = TaskEntity.builder()
            .taskName(TASK_0)
            .virtualQueue(VirtualQueue.READY)
            .workflowId(UUID.randomUUID())
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .build();
        unplannedTask = taskRepository.saveOrUpdate(unplannedTask);
        TaskEntity unplannedTaskWithNotToPlanFlag = TaskEntity.builder()
            .taskName(TASK_0)
            .virtualQueue(VirtualQueue.READY)
            .workflowId(UUID.randomUUID())
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .notToPlan(true)
            .build();
        unplannedTaskWithNotToPlanFlag = taskRepository.saveOrUpdate(unplannedTaskWithNotToPlanFlag);
        log.info("shouldNotPlanWhenNotToPlanIsSet(): finished population");

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        TaskEntity finalUnplannedTask = unplannedTask;
        waitFor(() -> taskRepository.find(finalUnplannedTask.getId())
            .map(shortTaskEntity -> clusterProvider.nodeId().equals(shortTaskEntity.getAssignedWorker()))
            .orElse(false)
        );
        Assertions.assertThat(taskRepository.find(unplannedTaskWithNotToPlanFlag.getId()))
            .map(shortTaskEntity -> shortTaskEntity.getAssignedWorker() == null)
            .isPresent()
            .contains(true);
    }

    //canceled flag is handled by worker itself
    @Test
    void shouldPlanWhenCanceledFlagIsSet() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef(TASK_0, String.class);
        waitForNodeIsRegistered(taskDef);
        registerPartition(TASK_0);

        log.info("shouldPlanWhenCanceledFlag(): begging of population");
        TaskEntity unplannedTask = TaskEntity.builder()
            .taskName(TASK_0)
            .virtualQueue(VirtualQueue.READY)
            .workflowId(UUID.randomUUID())
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .canceled(true)
            .build();
        unplannedTask = taskRepository.saveOrUpdate(unplannedTask);
        log.info("shouldPlanWhenCanceledFlag(): finished population");

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        verifyAllHaveCurrentNodeWorker(List.of(unplannedTask));
    }

    //maxParallelTasksInNode(10)
    //planFactor(2.F)
    @Test
    void shouldPlanFairlyWhenDifferentTasksInOneGroup() {
        //when
        TaskDef<String> taskDef1 = TaskDef.privateTaskDef(TASK_0, String.class);
        TaskDef<String> taskDef2 = TaskDef.privateTaskDef(TASK_1, String.class);
        waitForNodeIsRegistered(taskDef1, taskDef2);
        registerPartition(AFG_1, TASK_0);
        registerPartition(AFG_1, TASK_1);

        log.info("shouldPlanFairlyWhenDifferentTasks(): begging of population");
        var firstDateTime = LocalDateTime.now(clock).minusHours(2);
        var firstPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 1), oneWithAffinityGroupAndTaskNameAndCreatedDate(AFG_1, TASK_0, firstDateTime))
        );
        var firstTasks = taskPopulateAndVerify.populate(0, 20, VirtualQueue.READY, firstPopulationSpecs);

        var secondDateTime = LocalDateTime.now(clock).minusHours(1);
        var secondPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 1), oneWithAffinityGroupAndTaskNameAndCreatedDate(AFG_1, TASK_1, secondDateTime))
        );
        var secondTasks = taskPopulateAndVerify.populate(0, 20, VirtualQueue.READY, secondPopulationSpecs);
        log.info("shouldPlanFairlyWhenDifferentTasks(): finished population");

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        verifyAllHaveCurrentNodeWorker(firstTasks.stream().limit(10).toList());
        verifyAllHaveCurrentNodeWorker(secondTasks.stream().limit(10).toList());

        verifyAllHaveNotWorker(firstTasks.stream().skip(10).toList());
        verifyAllHaveNotWorker(secondTasks.stream().skip(10).toList());
    }

    //maxParallelTasksInNode(10)
    //planFactor(2.F)
    @Test
    void shouldPlanFairlyWhenDifferentAffinityGroup() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef(TASK_0, String.class);
        waitForNodeIsRegistered(taskDef);
        registerPartition(AFG_1, TASK_0);
        registerPartition(AFG_2, TASK_0);

        log.info("shouldPlanFairlyWhenDifferentTasks(): begging of population");
        var firstDateTime = LocalDateTime.now(clock).minusHours(2);
        var firstPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 1), oneWithAffinityGroupAndTaskNameAndCreatedDate(AFG_1, TASK_0, firstDateTime))
        );
        var firstTasks = taskPopulateAndVerify.populate(0, 20, VirtualQueue.READY, firstPopulationSpecs);

        var secondDateTime = LocalDateTime.now(clock).minusHours(1);
        var secondPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 1), oneWithAffinityGroupAndTaskNameAndCreatedDate(AFG_2, TASK_0, secondDateTime))
        );
        var secondTasks = taskPopulateAndVerify.populate(0, 20, VirtualQueue.READY, secondPopulationSpecs);
        log.info("shouldPlanFairlyWhenDifferentTasks(): finished population");

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        verifyAllHaveCurrentNodeWorker(firstTasks.stream().limit(10).toList());
        verifyAllHaveCurrentNodeWorker(secondTasks.stream().limit(10).toList());

        verifyAllHaveNotWorker(firstTasks.stream().skip(10).toList());
        verifyAllHaveNotWorker(secondTasks.stream().skip(10).toList());
    }

    //maxParallelTasksInNode(10)
    //planFactor(2.F)
    @Test
    void shouldNotPlanAndHandleLimitsCorrectlyWhenOneNodeIsOverloaded() {
        //when
        doReturn(Map.of(
            NODE0, Set.of(TASK_0, TASK_1),
            NODE1, Set.of(TASK_0, TASK_1),
            NODE2, Set.of(TASK_0, TASK_1, TASK_2) //TASK_2 hasn't to be planned
        ))
            .when(taskRegistryService).getRegisteredLocalTaskInCluster();

        doReturn(Optional.of(TaskSettings.builder()
            .maxParallelInCluster(10)
            .build())
        ).when(taskRegistryService).getLocalTaskParameters(eq(TASK_0));

        doReturn(Optional.of(TaskSettings.builder()
            .maxParallelInCluster(10)
            .build())
        ).when(taskRegistryService).getLocalTaskParameters(eq(TASK_1));

        doReturn(Optional.of(TaskSettings.builder()
            .maxParallelInCluster(10)
            .build())
        ).when(taskRegistryService).getLocalTaskParameters(eq(TASK_2));

        doReturn(NODE0).when(clusterProvider).nodeId();
        Mockito.doReturn(List.of(
            NodeLoading.builder().node(NODE0).medianCpuLoading(0.10).build(),
            NodeLoading.builder().node(NODE1).medianCpuLoading(0.10).build(),
            NodeLoading.builder().node(NODE2).medianCpuLoading(0.98).build() //node is overloaded
        )).when(clusterProvider).currentNodeLoading();

        registerPartition(List.of(TASK_0, TASK_1, TASK_2));

        log.info("shouldNotPlanAndHandleLimitsCorrectlyWhenOneNodeIsOverloaded(): begging of population");
        var alreadyPlannedPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 1), withWorkerAndWithoutAffinity(NODE0, 3),
                Range.closedOpen(1, 2), withWorkerAndWithoutAffinity(NODE1, 3),
                Range.closedOpen(2, 3), withWorkerAndWithoutAffinity(NODE2, 3)
            )
        );
        taskPopulateAndVerify.populate(
            0,
            18, //3 tasks for each spec * 3 spec = 9. let to be tasks 6 for each spec: 9*2 = 18
            VirtualQueue.READY,
            alreadyPlannedPopulationSpecs
        );

        var unplannedPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 3), withCreatedDateAndWithoutAffinity(3, LocalDateTime.now(clock)))
        );
        var unplannedTasks = taskPopulateAndVerify.populate(
            0,
            100,
            VirtualQueue.READY,
            unplannedPopulationSpecs
        );
        log.info("shouldNotPlanAndHandleLimitsCorrectlyWhenOneNodeIsOverloaded(): finished population");

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(plannerService::planningLoop));

        //verify
        verifyAllHaveAnyNodeWorker(unplannedTasks.stream()
                .filter(taskEntity -> TASK_0.equals(taskEntity.getTaskName()))
                .limit(4)
                .toList(),
            Set.of(NODE0, NODE1)
        );
        verifyAllHaveAnyNodeWorker(unplannedTasks.stream()
                .filter(taskEntity -> TASK_1.equals(taskEntity.getTaskName()))
                .limit(4)
                .toList(),
            Set.of(NODE0, NODE1)
        );

        verifyAllHaveNotWorker(unplannedTasks.stream()
            .filter(taskEntity -> TASK_0.equals(taskEntity.getTaskName()))
            .skip(4)
            .toList()
        );
        verifyAllHaveNotWorker(unplannedTasks.stream()
            .filter(taskEntity -> TASK_1.equals(taskEntity.getTaskName()))
            .skip(4)
            .toList()
        );
        verifyAllHaveNotWorker(unplannedTasks.stream()
            .filter(taskEntity -> TASK_2.equals(taskEntity.getTaskName()))
            .toList()
        );
    }

    private AtomicInteger spyProcessInLoopInvocation() {
        AtomicInteger flag = new AtomicInteger(0);
        doAnswer(invocation -> {
            try {
                return invocation.callRealMethod();
            } finally {
                flag.incrementAndGet();
            }
        }).when(plannerService).processInLoop();
        return flag;
    }

    private void registerPartition(String taskName) {
        registerPartition(null, taskName);
    }

    private void registerPartition(Collection<String> taskNames) {
        taskNames.forEach(taskName -> registerPartition(null, taskName));
    }

    private void registerPartition(@Nullable String affinityGroup, String taskName) {
        partitionRepository.save(PartitionEntity.builder()
            .affinityGroup(affinityGroup)
            .taskName(taskName)
            .timeBucket(1L)
            .build()
        );
    }

    private void verifyAllHaveNotWorker(Collection<TaskEntity> taskEntitiesToVerify) {
        verifyAllHaveAnyNodeWorker(taskEntitiesToVerify, Collections.emptySet());
    }

    private void verifyAllHaveCurrentNodeWorker(Collection<TaskEntity> taskEntitiesToVerify) {
        verifyAllHaveAnyNodeWorker(taskEntitiesToVerify, Set.of(clusterProvider.nodeId()));
    }

    private void verifyAllHaveAnyNodeWorker(Collection<TaskEntity> taskEntitiesToVerify, Set<UUID> workers) {
        Set<UUID> taskIds = taskEntitiesToVerify.stream().map(TaskEntity::getId).collect(Collectors.toSet());
        Set<String> taskNames = taskEntitiesToVerify.stream().map(TaskEntity::getTaskName).collect(Collectors.toSet());
        waitFor(() -> {
                List<TaskEntity> taskEntities = taskNames.stream()
                    .flatMap(taskName -> taskRepository.findAllByTaskName(taskName).stream())
                    .filter(shortTaskEntity -> taskIds.contains(shortTaskEntity.getId()))
                    .toList();
                return taskEntities.size() == taskIds.size() &&
                    taskEntities.stream()
                        .allMatch(t -> workers.isEmpty() && t.getAssignedWorker() == null ||
                            t.getAssignedWorker() != null && workers.contains(t.getAssignedWorker()));
            }
        );
    }
}