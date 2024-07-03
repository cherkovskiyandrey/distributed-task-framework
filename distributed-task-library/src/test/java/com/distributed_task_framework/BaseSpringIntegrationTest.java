package com.distributed_task_framework;

import com.distributed_task_framework.mapper.PartitionMapper;
import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.NodeLoading;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.PartitionEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.TaskLinkEntity;
import com.distributed_task_framework.persistence.entity.TaskMessageEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.CapabilityRepository;
import com.distributed_task_framework.persistence.repository.DlcRepository;
import com.distributed_task_framework.persistence.repository.DltRepository;
import com.distributed_task_framework.persistence.repository.NodeStateRepository;
import com.distributed_task_framework.persistence.repository.PartitionRepository;
import com.distributed_task_framework.persistence.repository.PlannerRepository;
import com.distributed_task_framework.persistence.repository.RegisteredTaskRepository;
import com.distributed_task_framework.persistence.repository.RemoteCommandRepository;
import com.distributed_task_framework.persistence.repository.RemoteTaskWorkerRepository;
import com.distributed_task_framework.persistence.repository.TaskLinkRepository;
import com.distributed_task_framework.persistence.repository.TaskMessageRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskGenerator;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.util.Pair;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
@Import(BaseSpringIntegrationTest.AdditionalTestConfiguration.class)
public abstract class BaseSpringIntegrationTest extends BaseTestContainerTest {
    protected static final Comparator<LocalDateTime> LOCAL_DATE_TIME_COMPARATOR_TO_SECONDS = Comparator.comparing(a -> a.truncatedTo(ChronoUnit.SECONDS));

    @Autowired
    TestClock clock;
    @Autowired
    CommonSettings commonSettings;
    @Autowired
    NodeStateRepository nodeStateRepository;
    @Autowired
    TaskRepository taskRepository;
    @Autowired
    RegisteredTaskRepository registeredTaskRepository;
    @Autowired
    PlannerRepository plannerRepository;
    @Autowired
    DltRepository dltRepository;
    @Autowired
    RemoteTaskWorkerRepository remoteTaskWorkerRepository;
    @Autowired
    RemoteCommandRepository remoteCommandRepository;
    @Autowired
    DlcRepository dlcRepository;
    @Autowired
    TaskSettings defaultTaskSettings;
    @SpyBean
    TaskRegistryService taskRegistryService;
    @SpyBean
    ClusterProvider clusterProvider;
    @Autowired
    TaskLinkRepository taskLinkRepository;
    @Autowired
    TaskMessageRepository taskMessageRepository;
    @Autowired
    PartitionRepository partitionRepository;
    @Autowired
    CapabilityRepository capabilityRepository;
    @Autowired
    TaskSerializer taskSerializer;
    @Autowired
    TaskMapper taskMapper;
    @Autowired
    PartitionMapper partitionMapper;
    @SpyBean
    WorkerContextManager workerContextManager;
    @Autowired
    @Qualifier("commonRegistryCacheManager")
    CacheManager commonRegistryCacheManager;

    @BeforeEach
    public void init() {
        clock.setClock(Clock.systemUTC());
        nodeStateRepository.deleteAll();
        taskRepository.deleteAll();
        registeredTaskRepository.deleteAll();
        plannerRepository.deleteAll();
        dltRepository.deleteAll();
        remoteTaskWorkerRepository.deleteAll();
        remoteCommandRepository.deleteAll();
        dlcRepository.deleteAll();
        taskLinkRepository.deleteAll();
        taskMessageRepository.deleteAll();
        partitionRepository.deleteAll();
        capabilityRepository.deleteAll();
        commonRegistryCacheManager.getCacheNames()
                .forEach(cacheName -> Objects.requireNonNull(commonRegistryCacheManager.getCache(cacheName)).clear());
    }

    protected void setFixedTime() {
        clock.setClock(Clock.fixed(Instant.ofEpochSecond(0), ZoneId.of("UTC")));
    }

    protected void setFixedTime(long epochSecond) {
        clock.setClock(Clock.fixed(Instant.ofEpochSecond(epochSecond), ZoneId.of("UTC")));
    }

    protected void waitFor(Callable<Boolean> conditionEvaluator) {
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .until(conditionEvaluator);
    }

    protected static <T> T waitAndGet(final Callable<T> supplier, final Predicate<? super T> predicate) {
        return await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .until(
                        supplier,
                        predicate
                );
    }

    protected void waitForNodeIsRegistered(TaskDef<?>... taskDefs) {
        var arguments = Arrays.stream(taskDefs)
                .map(taskDef -> Pair.<TaskDef<?>, TaskSettings>of(taskDef, defaultTaskSettings))
                .collect(Collectors.toList());
        waitForNodeIsRegistered(arguments);
    }

    protected void waitForNodeIsRegistered(List<Pair<TaskDef<?>, TaskSettings>> tasksWithProperties) {
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> nodeStateRepository.findById(clusterProvider.nodeId()).isPresent());
        for (Pair<TaskDef<?>, TaskSettings> taskWithProperties : tasksWithProperties) {
            Task<?> task = TaskGenerator.defineTask(taskWithProperties.getFirst(), m -> System.out.println("hello world!"));
            taskRegistryService.registerTask(task, taskWithProperties.getSecond());
            waitFor(() -> registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId()).stream()
                    .anyMatch(registeredTaskEntity -> registeredTaskEntity.getTaskName().equals(
                            taskWithProperties.getFirst().getTaskName())
                    )
            );
        }
    }

    protected void waitForTasksUnregistered(TaskDef<?>... taskDefs) {
        for (TaskDef<?> taskDef : taskDefs) {
            taskRegistryService.unregisterLocalTask(taskDef.getTaskName());
            waitFor(() -> registeredTaskRepository.findByNodeStateId(clusterProvider.nodeId()).stream()
                    .noneMatch(registeredTaskEntity -> registeredTaskEntity.getTaskName().equals(taskDef.getTaskName())));
        }
    }

    protected TaskLinkEntity toJoinTaskLink(TaskId joinTask, TaskId taskId) {
        return TaskLinkEntity.builder()
                .joinTaskName(joinTask.getTaskName())
                .joinTaskId(joinTask.getId())
                .taskToJoinId(taskId.getId())
                .build();
    }

    protected void mockNodeCpuLoading(double cpuLoading) {
        assertThat(cpuLoading).isGreaterThan(0.).isLessThan(1.);
        Mockito.doAnswer(invocation -> {
                    List<NodeLoading> realResult = (List<NodeLoading>) invocation.callRealMethod();
                    return realResult.stream()
                            .map(nodeLoading -> nodeLoading.toBuilder().medianCpuLoading(cpuLoading).build())
                            .toList();
                })
                .when(clusterProvider).currentNodeLoading();
    }

    protected TaskId createTaskId(String taskName) {
        return TaskId.builder()
                .appName("test-app")
                .id(UUID.randomUUID())
                .taskName(taskName)
                .build();
    }

    protected TaskId createAndRegisterJoinTask(String taskName) {
        TaskEntity taskEntity = TaskEntity.builder()
                .taskName(taskName)
                .workflowId(UUID.randomUUID())
                .virtualQueue(VirtualQueue.NEW)
                .workflowCreatedDateUtc(LocalDateTime.now(clock))
                .executionDateUtc(LocalDateTime.now(clock))
                .notToPlan(true)
                .build();
        taskEntity = taskRepository.saveOrUpdate(taskEntity);
        return taskMapper.map(taskEntity, "test-app");
    }

    protected void markAsReady(TaskId taskId) {
        taskLinkRepository.markLinksAsCompleted(taskId.getId());
    }

    @SneakyThrows
    protected TaskMessageEntity toMessage(TaskId from, TaskId to, String message) {
        return TaskMessageEntity.builder()
                .taskToJoinId(from.getId())
                .joinTaskId(to.getId())
                .message(taskSerializer.writeValue(message))
                .build();
    }

    protected TaskSettings newRecurrentTaskSettings() {
        return defaultTaskSettings.toBuilder()
                .cron("*/50 * * * * *")
                .build();
    }

    protected Collection<TaskEntity> filterAssigned(Collection<TaskEntity> shortTaskEntities) {
        return shortTaskEntities.stream()
                .filter(shortTaskEntity -> shortTaskEntity.getAssignedWorker() != null)
                .collect(Collectors.toList());
    }

    protected void verifyRegisteredPartitionFromTask(Collection<TaskEntity> taskEntities) {
        var expectedPartitions = taskEntities.stream()
                .map(taskMapper::mapToPartition)
                .toList();
        var actualPartitions = Lists.newArrayList(partitionRepository.findAll()).stream()
                .map(partitionMapper::fromEntity)
                .toList();
        assertThat(actualPartitions).containsExactlyInAnyOrderElementsOf(expectedPartitions);
    }

    protected void verifyRegisteredPartition(Collection<Partition> expectedPartitions) {
        var actualPartitions = Lists.newArrayList(partitionRepository.findAll()).stream()
                .map(partitionMapper::fromEntity)
                .collect(Collectors.toSet());
        assertThat(actualPartitions).containsExactlyInAnyOrderElementsOf(expectedPartitions);
    }

    protected PartitionEntity createPartition(String affinityGroup, String taskName, long timeBucket) {
        return PartitionEntity.builder()
                .affinityGroup(affinityGroup)
                .taskName(taskName)
                .timeBucket(timeBucket)
                .build();
    }

    protected TaskId toTaskId(TaskEntity taskEntity) {
        return taskMapper.map(taskEntity, commonSettings.getAppName());
    }

    protected Collection<UUID> toIds(Collection<TaskEntity> tasks) {
        return tasks.stream().map(TaskEntity::getId).toList();
    }

    @TestConfiguration
    public static class AdditionalTestConfiguration {

        @Bean
        public TaskPopulateAndVerify taskPopulate() {
            return new TaskPopulateAndVerify();
        }
    }
}
