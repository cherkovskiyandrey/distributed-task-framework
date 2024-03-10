package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.service.internal.TaskWorkerFactory;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskWorkerGenerator;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class WorkerManagerImplIntegrationTest extends BaseSpringIntegrationTest {
    private static final UUID NODE_ID = UUID.randomUUID();
    private static final String KNOWN_TASK_NAME = "KNOWN_TASK_NAME";
    private static final String TASK_WITH_TIMEOUT_NAME = "TASK_WITH_TIMEOUT_NAME";
    private static final String UNKNOWN_TASK_NAME = "UNKNOWN_TASK_NAME";
    private static final Duration TASK_TIMEOUT = Duration.ofSeconds(10);

    //turn off real workerManager
    @MockBean
    WorkerManager workerManagerReal;
    @MockBean
    TaskWorkerFactory taskWorkerFactory;
    @MockBean
    TaskRegistryService taskRegistryService;
    @Autowired
    TaskRepository taskRepository;
    @Autowired
    TaskMapper taskMapper;
    @Autowired
    MetricHelper metricHelper;
    WorkerManagerImpl workerManager;
    ExecutorService executorService;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void init() {
        super.init();
        executorService = Executors.newSingleThreadExecutor();
        when(clusterProvider.nodeId()).thenReturn(NODE_ID);

        when(taskRegistryService.getRegisteredLocalTask(eq(KNOWN_TASK_NAME)))
                .thenReturn(Optional.of(RegisteredTask.of(
                        (Task<Object>) mock(Task.class),
                        defaultTaskSettings.toBuilder().build()
                )));
        when(taskRegistryService.getRegisteredLocalTask(eq(TASK_WITH_TIMEOUT_NAME)))
                .thenReturn(Optional.of(RegisteredTask.of(
                        (Task<Object>) mock(Task.class),
                        defaultTaskSettings.toBuilder()
                                .timeout(TASK_TIMEOUT)
                                .build()
                )));
        when(taskRegistryService.getRegisteredLocalTask(eq(UNKNOWN_TASK_NAME)))
                .thenReturn(Optional.empty());

        TaskWorker taskWorker = TaskWorkerGenerator.defineTaskWorker((taskEntity, registeredTask) -> {
            taskRepository.delete(taskEntity);
            log.info("execute(): deleted task=[{}]", taskEntity);
        });
        when(taskWorkerFactory.buildTaskWorker(any(TaskEntity.class), any(TaskSettings.class))).thenReturn(taskWorker);
        workerManager = Mockito.spy(new WorkerManagerImpl(
                commonSettings,
                clusterProvider,
                taskRegistryService,
                taskWorkerFactory,
                taskRepository,
                taskMapper,
                clock,
                metricHelper
        ));
    }

    @SneakyThrows
    @AfterEach
    void shutdown() {
        workerManager.shutdown();
        executorService.shutdownNow();
        //noinspection ResultOfMethodCallIgnored
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    void shouldExecuteNewTasks() {
        //when
        List<TaskEntity> newTasksToExecute = IntStream.range(0, 100)
                .mapToObj(i -> TaskEntity.builder()
                        .taskName(KNOWN_TASK_NAME)
                        .virtualQueue(VirtualQueue.NEW)
                        .workflowId(UUID.randomUUID())
                        .workflowCreatedDateUtc(LocalDateTime.now(clock))
                        .assignedWorker(NODE_ID)
                        .executionDateUtc(LocalDateTime.now(clock))
                        .build()
                )
                .collect(Collectors.toList());
        taskRepository.saveAll(newTasksToExecute);

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(workerManager::manageLoop));

        //verify
        waitFor(() -> taskRepository.findAllByTaskName(KNOWN_TASK_NAME).isEmpty());
        waitFor(() -> workerManager.getCurrentActiveTasks() == 0L);
    }

    @Test
    void shouldNotExecuteForeignNewTasks() {
        //when
        List<TaskEntity> newUnknownTasksToExecute = IntStream.range(0, 100)
                .mapToObj(i -> TaskEntity.builder()
                        .taskName(UNKNOWN_TASK_NAME)
                        .virtualQueue(VirtualQueue.NEW)
                        .workflowId(UUID.randomUUID())
                        .workflowCreatedDateUtc(LocalDateTime.now(clock))
                        .assignedWorker(NODE_ID)
                        .executionDateUtc(LocalDateTime.now(clock))
                        .build()
                )
                .collect(Collectors.toList());
        taskRepository.saveAll(newUnknownTasksToExecute);

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(workerManager::manageLoop));

        //verify
        waitFor(() -> taskRepository.findAllByTaskName(UNKNOWN_TASK_NAME).stream()
                .allMatch(shortTaskEntity -> shortTaskEntity.getAssignedWorker() == null &&
                        shortTaskEntity.getLastAssignedDateUtc() == null)
        );
        waitFor(() -> workerManager.getCurrentActiveTasks() == 0L);
    }

    @SneakyThrows
    @Test
    void shouldNotExecutedWhenAlreadyInProgressTasks() {
        //when
        TaskEntity alreadyInProgressTask = taskRepository.saveOrUpdate(TaskEntity.builder()
                .taskName(KNOWN_TASK_NAME)
                .virtualQueue(VirtualQueue.NEW)
                .workflowId(UUID.randomUUID())
                .workflowCreatedDateUtc(LocalDateTime.now(clock))
                .assignedWorker(NODE_ID)
                .executionDateUtc(LocalDateTime.now(clock))
                .build()
        );

        List<TaskEntity> newTasksToExecute = IntStream.range(0, 50)
                .mapToObj(i -> TaskEntity.builder()
                        .taskName(KNOWN_TASK_NAME)
                        .virtualQueue(VirtualQueue.NEW)
                        .workflowId(UUID.randomUUID())
                        .workflowCreatedDateUtc(LocalDateTime.now(clock))
                        .assignedWorker(NODE_ID)
                        .executionDateUtc(LocalDateTime.now(clock))
                        .build()
                )
                .collect(Collectors.toList());
        taskRepository.saveAll(newTasksToExecute);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        TaskWorker taskWorker = TaskWorkerGenerator.defineTaskWorker((taskEntity, registeredTask) -> {
            if (taskEntity.getId().equals(alreadyInProgressTask.getId())) {
                log.info("Before waiting");
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    log.error("Waiting error: ", e);
                    throw new RuntimeException(e);
                }
                log.info("After waiting");
            }
            taskRepository.delete(taskEntity);
            log.info("execute(): deleted task=[{}]", taskEntity);
        });
        when(taskWorkerFactory.buildTaskWorker(any(TaskEntity.class), any(TaskSettings.class))).thenReturn(taskWorker);
        AtomicInteger invocationVerifier = spyProcessInLoopInvocation();

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(workerManager::manageLoop));

        //verify
        waitFor(() -> invocationVerifier.get() > 1);
        waitFor(() -> taskRepository.findAllByTaskName(KNOWN_TASK_NAME).size() == 1);
        countDownLatch.countDown();
        waitFor(() -> taskRepository.findAllByTaskName(KNOWN_TASK_NAME).isEmpty());
        waitFor(() -> workerManager.getCurrentActiveTasks() == 0L);
    }

    @Test
    void shouldRunNotMoreThanMaxInParallelAndPreserveOrdering() {
        //when
        List<TaskEntity> newEarliestTasksToExecute = IntStream.range(0, 10)
                .mapToObj(i -> TaskEntity.builder()
                        .taskName(KNOWN_TASK_NAME)
                        .virtualQueue(VirtualQueue.NEW)
                        .workflowId(UUID.randomUUID())
                        .workflowCreatedDateUtc(LocalDateTime.now(clock))
                        .assignedWorker(NODE_ID)
                        .executionDateUtc(LocalDateTime.now(clock))
                        .build()
                )
                .collect(Collectors.toList());
        Set<UUID> newEarliestTaskIdsToExecute = taskRepository.saveAll(newEarliestTasksToExecute).stream()
                .map(TaskEntity::getId)
                .collect(Collectors.toSet());

        List<TaskEntity> newLatestTasksToExecute = IntStream.range(0, 10)
                .mapToObj(i -> TaskEntity.builder()
                        .taskName(KNOWN_TASK_NAME)
                        .virtualQueue(VirtualQueue.NEW)
                        .workflowId(UUID.randomUUID())
                        .workflowCreatedDateUtc(LocalDateTime.now(clock))
                        .assignedWorker(NODE_ID)
                        .executionDateUtc(LocalDateTime.now(clock))
                        .build()
                )
                .collect(Collectors.toList());
        Set<UUID> newLatestTaskIdsToExecute = taskRepository.saveAll(newLatestTasksToExecute).stream()
                .map(TaskEntity::getId)
                .collect(Collectors.toSet());

        ConcurrentLinkedQueue<UUID> executedIds = new ConcurrentLinkedQueue<>();
        CyclicBarrier cyclicBarrier = new CyclicBarrier(10, () -> log.info("Batch has been processed!"));

        TaskWorker taskWorker = TaskWorkerGenerator.defineTaskWorker((taskEntity, registeredTask) -> {
            executedIds.add(taskEntity.getId());
            log.info("Before barrier");
            try {
                cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
            taskRepository.delete(taskEntity);
            log.info("execute(): deleted task=[{}]", taskEntity);
        });
        when(taskWorkerFactory.buildTaskWorker(any(TaskEntity.class), any(TaskSettings.class))).thenReturn(taskWorker);

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(workerManager::manageLoop));

        //verify
        waitFor(() -> taskRepository.findAllByTaskName(KNOWN_TASK_NAME).isEmpty());
        waitFor(() -> workerManager.getCurrentActiveTasks() == 0L);

        List<UUID> executedIdList = Lists.newArrayList(executedIds);
        Set<UUID> realEarliestTaskIds = Sets.newHashSet(executedIdList.subList(0, 10));
        Set<UUID> realLatestTaskIds = Sets.newHashSet(executedIdList.subList(10, 20));

        assertThat(Sets.intersection(newEarliestTaskIdsToExecute, realEarliestTaskIds)).hasSize(10);
        assertThat(Sets.intersection(newLatestTaskIdsToExecute, realLatestTaskIds)).hasSize(10);
    }

    @Test
    void shouldBeInterruptedWhenTimeout() {
        //when
        setFixedTime();
        taskRepository.saveOrUpdate(TaskEntity.builder()
                .taskName(TASK_WITH_TIMEOUT_NAME)
                .virtualQueue(VirtualQueue.NEW)
                .workflowId(UUID.randomUUID())
                .workflowCreatedDateUtc(LocalDateTime.now(clock))
                .assignedWorker(NODE_ID)
                .executionDateUtc(LocalDateTime.now(clock))
                .build()
        );
        AtomicBoolean hasBeenInterrupted = new AtomicBoolean(false);
        CountDownLatch waitToAllowToDelete = new CountDownLatch(1);
        TaskWorker taskWorker = TaskWorkerGenerator.defineTaskWorker((taskEntity, registeredTask) -> {
            try {
                TimeUnit.SECONDS.sleep(100);
            } catch (InterruptedException e) {
                hasBeenInterrupted.set(true);
            }

            try {
                waitToAllowToDelete.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            taskRepository.find(taskEntity.getId()).ifPresent(taskRepository::delete);
            log.info("execute(): deleted task=[{}]", taskEntity);
        });
        when(taskWorkerFactory.buildTaskWorker(any(TaskEntity.class), any(TaskSettings.class))).thenReturn(taskWorker);
        AtomicInteger invocationVerifier = spyProcessInLoopInvocation();

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(workerManager::manageLoop));

        //verify
        waitFor(() -> invocationVerifier.get() > 1);
        waitFor(() -> taskRepository.findAllByTaskName(TASK_WITH_TIMEOUT_NAME).size() == 1);

        setFixedTime(TASK_TIMEOUT.getSeconds() + 1);
        waitFor(() -> Boolean.TRUE.equals(hasBeenInterrupted.get()));
        int currentCycleNumber = invocationVerifier.get();
        waitFor(() -> invocationVerifier.get() > currentCycleNumber + 1);
        assertThat(workerManager.getCurrentActiveTasks()).isEqualTo(1);
        waitToAllowToDelete.countDown();
        waitFor(() -> taskRepository.findAllByTaskName(TASK_WITH_TIMEOUT_NAME).isEmpty());
        waitFor(() -> workerManager.getCurrentActiveTasks() == 0L);
    }

    @SneakyThrows
    @Test
    void shouldBeInterruptedWhenCanceled() {
        //when
        TaskEntity newTaskEntity = taskRepository.saveOrUpdate(TaskEntity.builder()
                .taskName(KNOWN_TASK_NAME)
                .virtualQueue(VirtualQueue.NEW)
                .workflowId(UUID.randomUUID())
                .workflowCreatedDateUtc(LocalDateTime.now(clock))
                .assignedWorker(NODE_ID)
                .executionDateUtc(LocalDateTime.now(clock))
                .build()
        );
        AtomicBoolean hasBeenInterrupted = new AtomicBoolean(false);
        CountDownLatch waitToAllowToDelete = new CountDownLatch(1);
        TaskWorker taskWorker = TaskWorkerGenerator.defineTaskWorker((taskEntity, registeredTask) -> {
            try {
                TimeUnit.SECONDS.sleep(100);
            } catch (InterruptedException e) {
                log.info("execute(): task has been interrupted task=[{}]", taskEntity);
                hasBeenInterrupted.set(true);
            }

            try {
                waitToAllowToDelete.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            taskRepository.find(taskEntity.getId()).ifPresent(taskRepository::delete);
            log.info("execute(): deleted task=[{}]", taskEntity);
        });
        when(taskWorkerFactory.buildTaskWorker(any(TaskEntity.class), any(TaskSettings.class))).thenReturn(taskWorker);
        AtomicInteger invocationVerifier = spyProcessInLoopInvocation();

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(workerManager::manageLoop));

        //verify
        waitFor(() -> invocationVerifier.get() > 1);
        waitFor(() -> taskRepository.findAllByTaskName(KNOWN_TASK_NAME).size() == 1);
        taskRepository.cancel(newTaskEntity.getId());
        waitFor(() -> Boolean.TRUE.equals(hasBeenInterrupted.get()));
        int currentCycleNumber = invocationVerifier.get();
        waitFor(() -> invocationVerifier.get() > currentCycleNumber + 1);
        assertThat(workerManager.getCurrentActiveTasks()).isEqualTo(1);
        waitToAllowToDelete.countDown();
        waitFor(() -> taskRepository.findAllByTaskName(KNOWN_TASK_NAME).isEmpty());
        waitFor(() -> workerManager.getCurrentActiveTasks() == 0L);
    }

    private AtomicInteger spyProcessInLoopInvocation() {
        AtomicInteger flag = new AtomicInteger(0);
        doAnswer(invocation -> {
            try {
                return invocation.callRealMethod();
            } finally {
                flag.incrementAndGet();
            }
        }).when(workerManager).manageTasks();
        return flag;
    }
}