package com.distributed_task_framework;

import com.distributed_task_framework.mapper.CommandMapper;
import com.distributed_task_framework.mapper.NodeStateMapper;
import com.distributed_task_framework.mapper.PartitionMapper;
import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.persistence.entity.CapabilityEntity;
import com.distributed_task_framework.persistence.entity.PartitionEntity;
import com.distributed_task_framework.persistence.entity.PlannerEntity;
import com.distributed_task_framework.persistence.entity.RegisteredTaskEntity;
import com.distributed_task_framework.persistence.entity.RemoteCommandEntity;
import com.distributed_task_framework.persistence.entity.RemoteTaskWorkerEntity;
import com.distributed_task_framework.persistence.entity.TaskLinkEntity;
import com.distributed_task_framework.persistence.entity.TaskMessageEntity;
import com.distributed_task_framework.persistence.repository.CapabilityRepository;
import com.distributed_task_framework.persistence.repository.DltRepository;
import com.distributed_task_framework.persistence.repository.NodeStateRepository;
import com.distributed_task_framework.persistence.repository.PartitionRepository;
import com.distributed_task_framework.persistence.repository.RegisteredTaskRepository;
import com.distributed_task_framework.persistence.repository.RemoteCommandRepository;
import com.distributed_task_framework.persistence.repository.TaskLinkRepository;
import com.distributed_task_framework.persistence.repository.TaskMessageRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.impl.ClusterProviderImpl;
import com.distributed_task_framework.service.impl.CronService;
import com.distributed_task_framework.service.impl.DistributedTaskServiceImpl;
import com.distributed_task_framework.service.impl.InternalTaskCommandProxyService;
import com.distributed_task_framework.service.impl.JoinTaskStatHelper;
import com.distributed_task_framework.service.impl.JsonTaskSerializerImpl;
import com.distributed_task_framework.service.impl.LocalTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.MetricHelperImpl;
import com.distributed_task_framework.service.impl.PartitionTrackerImpl;
import com.distributed_task_framework.service.impl.RemoteTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.TaskCommandStatHelper;
import com.distributed_task_framework.service.impl.TaskLinkManagerImpl;
import com.distributed_task_framework.service.impl.TaskRegistryServiceImpl;
import com.distributed_task_framework.service.impl.TaskWorkerFactoryImpl;
import com.distributed_task_framework.service.impl.VirtualQueueBaseTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.VirtualQueueStatHelper;
import com.distributed_task_framework.service.impl.WorkerContextManagerImpl;
import com.distributed_task_framework.service.impl.WorkerManagerImpl;
import com.distributed_task_framework.service.impl.workers.LocalAtLeastOnceWorker;
import com.distributed_task_framework.service.impl.workers.LocalExactlyOnceWorker;
import com.distributed_task_framework.service.internal.CapabilityRegister;
import com.distributed_task_framework.service.internal.CapabilityRegisterProvider;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.service.internal.PlannerService;
import com.distributed_task_framework.service.internal.TaskCommandWithDetectorService;
import com.distributed_task_framework.service.internal.TaskLinkManager;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.service.internal.TaskWorkerFactory;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.Fixed;
import com.distributed_task_framework.settings.Retry;
import com.distributed_task_framework.settings.RetryMode;
import com.distributed_task_framework.settings.TaskSettings;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.mapstruct.factory.Mappers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jdbc.repository.config.EnableJdbcAuditing;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.net.URL;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@TestConfiguration
@EnableJdbcAuditing//(dateTimeProviderRef = "auditingDateTimeProvider")
@EnableJdbcRepositories(basePackageClasses = NodeStateRepository.class)
@EnableTransactionManagement
@EnableCaching
public class BaseTestConfiguration {
    @Bean
    public TestClock distributedTaskInternalClock() {
        return new TestClock();
    }

    @Bean
    public DateTimeProvider auditingDateTimeProvider(Clock clock) {
        return () -> Optional.of(LocalDateTime.now(clock));
    }

    @Bean
    public BeforeConvertCallback<PlannerEntity> plannerEntityBeforeConvertCallback() {
        return plannerEntity -> {
            if (plannerEntity.getId() == null) {
                plannerEntity.setId(UUID.randomUUID());
            }
            return plannerEntity;
        };
    }

    @Bean
    public BeforeConvertCallback<RegisteredTaskEntity> registeredTaskEntityBeforeConvertCallback() {
        return registeredTaskEntity -> {
            if (registeredTaskEntity.getId() == null) {
                registeredTaskEntity.setId(UUID.randomUUID());
            }
            return registeredTaskEntity;
        };
    }

    @Bean
    public BeforeConvertCallback<RemoteCommandEntity> remoteCommandEntityBeforeConvertCallback(Clock clock) {
        return remoteCommandEntity -> {
            if (remoteCommandEntity.getId() == null) {
                remoteCommandEntity.setId(UUID.randomUUID());
                remoteCommandEntity.setCreatedDateUtc(LocalDateTime.now(clock));
            }
            return remoteCommandEntity;
        };
    }

    @Bean
    public BeforeConvertCallback<RemoteTaskWorkerEntity> remoteTaskWorkerEntityBeforeConvertCallback() {
        return remoteCommandEntity -> {
            if (remoteCommandEntity.getId() == null) {
                remoteCommandEntity.setId(UUID.randomUUID());
            }
            return remoteCommandEntity;
        };
    }

    @Bean
    public BeforeConvertCallback<TaskLinkEntity> taskLinkEntityBeforeConvertCallback() {
        return taskLinkEntity -> {
            if (taskLinkEntity.getId() == null) {
                taskLinkEntity.setId(UUID.randomUUID());
            }
            return taskLinkEntity;
        };
    }

    @Bean
    public BeforeConvertCallback<TaskMessageEntity> taskMessageEntityBeforeConvertCallback() {
        return taskMessageEntity -> {
            if (taskMessageEntity.getId() == null) {
                taskMessageEntity.setId(UUID.randomUUID());
            }
            return taskMessageEntity;
        };
    }

    @Bean
    public BeforeConvertCallback<PartitionEntity> partitionEntityBeforeConvertCallback() {
        return partitionEntity -> {
            if (partitionEntity.getId() == null) {
                partitionEntity.setId(UUID.randomUUID());
            }
            return partitionEntity;
        };
    }

    @Bean
    public BeforeConvertCallback<CapabilityEntity> capabilityEntityBeforeConvertCallback() {
        return capabilityEntity -> {
            if (capabilityEntity.getId() == null) {
                capabilityEntity.setId(UUID.randomUUID());
            }
            return capabilityEntity;
        };
    }

    @Bean
    @Qualifier("commonRegistryCaffeineConfig")
    public Caffeine<Object, Object> commonRegistryCaffeineConfig(CommonSettings commonSettings) {
        return Caffeine.newBuilder()
                .expireAfterWrite(commonSettings.getRegistrySettings().getCacheExpirationMs(), TimeUnit.MILLISECONDS);
    }

    @Bean
    @Qualifier("commonRegistryCacheManager")
    public CacheManager commonRegistryCacheManager(@Qualifier("commonRegistryCaffeineConfig") Caffeine<Object, Object> caffeine) {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeine(caffeine);
        return caffeineCacheManager;
    }

    @Bean
    public CronService cronService(Clock clock) {
        return new CronService(clock);
    }

    @Bean
    public TaskMapper taskMapper() {
        return Mappers.getMapper(TaskMapper.class);
    }

    @Bean
    public NodeStateMapper nodeStateMapper() {
        return Mappers.getMapper(NodeStateMapper.class);
    }

    @Bean
    public CommandMapper commandMapper() {
        return Mappers.getMapper(CommandMapper.class);
    }

    @Bean
    public PartitionMapper partitionMapper() {
        return Mappers.getMapper(PartitionMapper.class);
    }

    @Bean
    @SneakyThrows
    public CommonSettings commonSettings() {
        return CommonSettings.DEFAULT.toBuilder()
                .appName("test-app")
                .registrySettings(CommonSettings.DEFAULT.getRegistrySettings().toBuilder()
                        .updateInitialDelayMs(1000)
                        .updateFixedDelayMs(1000)
                        .maxInactivityIntervalMs(5000)
                        .cacheExpirationMs(0)
                        .build()
                )
                .plannerSettings(CommonSettings.DEFAULT.getPlannerSettings().toBuilder()
                        .batchSize(100)
                        .fetchFactor(2.F)
                        .affinityGroupScannerTimeOverlap(Duration.ofSeconds(1))
                        .partitionTrackingTimeWindow(Duration.ofSeconds(5))
                        .planFactor(2.F)
                        .build()
                )
                .workerManagerSettings(CommonSettings.DEFAULT.getWorkerManagerSettings().toBuilder()
                        .maxParallelTasksInNode(10)
                        .build()
                )
                .deliveryManagerSettings(CommonSettings.DEFAULT.getDeliveryManagerSettings().toBuilder()
                        .batchSize(100)
                        .remoteApps(CommonSettings.RemoteApps.builder()
                                .appToUrl(Map.of("foreign-app", new URL("http://foreign-app:8080")))
                                .build()
                        )
                        .retry(Retry.builder()
                                .retryMode(RetryMode.FIXED)
                                .fixed(Fixed.builder()
                                        .delay(Duration.ofSeconds(1))
                                        .maxNumber(4)
                                        .build())
                                .build()
                        )
                        .build()
                )
                .build();
    }

    @Bean
    public TaskSettings defaultTaskSettingsForTestOnly() {
        return TaskSettings.DEFAULT.toBuilder()
                .retry(TaskSettings.DEFAULT.getRetry().toBuilder()
                        .retryMode(RetryMode.FIXED)
                        .build()
                )
                .build();
    }

    @Component
    @RequiredArgsConstructor
    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    public static class CapabilityRegisterProviderImpl implements CapabilityRegisterProvider {
        List<CapabilityRegister> capabilityRegisters;

        @Override
        public Collection<CapabilityRegister> getAllCapabilityRegister() {
            return capabilityRegisters;
        }
    }

    @Bean
    public ClusterProvider clusterProvider(CommonSettings commonSettings,
                                           @Lazy CapabilityRegisterProvider capabilityRegisterProvider,
                                           PlatformTransactionManager transactionManager,
                                           NodeStateMapper nodeStateMapper,
                                           CacheManager cacheManager,
                                           NodeStateRepository nodeStateRepository,
                                           CapabilityRepository capabilityRepository,
                                           Clock clock) {
        return new ClusterProviderImpl(
                commonSettings,
                capabilityRegisterProvider,
                transactionManager,
                cacheManager,
                nodeStateMapper,
                nodeStateRepository,
                capabilityRepository,
                clock
        );
    }

    @Bean
    public TaskRegistryService taskRegistryService(CommonSettings commonSettings,
                                                   RegisteredTaskRepository registeredTaskRepository,
                                                   PlatformTransactionManager transactionManager,
                                                   ClusterProvider clusterProvider) {
        return new TaskRegistryServiceImpl(
                commonSettings,
                registeredTaskRepository,
                transactionManager,
                clusterProvider
        );
    }

    @Bean
    public ObjectMapper objectMapper() {
        var objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        //in order to easily add new properties to the task message and be tolerant during rolling out
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        objectMapper.registerModule(new JavaTimeModule());
        return objectMapper;
    }

    @Bean
    public TaskSerializer taskSerializer(ObjectMapper objectMapper) {
        return new JsonTaskSerializerImpl(objectMapper);
    }

    @Bean
    public TaskLinkManager taskLinkManager(TaskLinkRepository taskLinkRepository,
                                           TaskMessageRepository taskMessageRepository,
                                           CommonSettings commonSettings,
                                           TaskSerializer taskSerializer) {
        return new TaskLinkManagerImpl(
                taskLinkRepository,
                taskMessageRepository,
                commonSettings,
                taskSerializer
        );
    }

    @Bean
    public MetricHelper metricHelper(MeterRegistry meterRegistry) {
        return new MetricHelperImpl(meterRegistry);
    }

    @Bean
    @Qualifier("fairGeneralTaskPlannerService")
    public PlannerService fairGeneralTaskPlannerService() {
        return Mockito.mock(PlannerService.class);
    }

    @Bean
    @Qualifier("virtualQueueManagerPlanner")
    public PlannerService virtualQueueManagerPlanner() {
        return Mockito.mock(PlannerService.class);
    }

    @Bean
    public VirtualQueueStatHelper virtualQueueStatHelper(@Lazy @Qualifier("virtualQueueManagerPlanner")
                                                         PlannerService plannerService,
                                                         CommonSettings commonSettings,
                                                         TaskRegistryService taskRegistryService,
                                                         TaskRepository taskRepository,
                                                         TaskMapper taskMapper,
                                                         MetricHelper metricHelper,
                                                         MeterRegistry meterRegistry) {
        return new VirtualQueueStatHelper(
                plannerService,
                commonSettings,
                taskRegistryService,
                taskRepository,
                taskMapper,
                metricHelper,
                meterRegistry
        );
    }

    @Bean
    public JoinTaskStatHelper joinTaskStatHelper(MetricHelper metricHelper) {
        return new JoinTaskStatHelper(metricHelper);
    }

    @Bean
    public WorkerContextManager workerContextManager() {
        return new WorkerContextManagerImpl();
    }

    @Bean
    public PartitionTracker partitionTracker(TaskRepository taskRepository,
                                             PartitionRepository partitionRepository,
                                             PartitionMapper partitionMapper,
                                             CommonSettings commonSettings,
                                             Clock clock) {
        return new PartitionTrackerImpl(
                taskRepository,
                partitionRepository,
                partitionMapper,
                commonSettings,
                clock
        );
    }

    @Bean
    @Qualifier("impl")
    public VirtualQueueBaseTaskCommandServiceImpl virtualQueueBaseTaskCommandService(PartitionTracker partitionTracker,
                                                                                     ClusterProvider clusterProvider,
                                                                                     TaskRepository taskRepository,
                                                                                     WorkerContextManager workerContextManager,
                                                                                     TaskMapper taskMapper) {
        return new VirtualQueueBaseTaskCommandServiceImpl(
                partitionTracker,
                clusterProvider,
                taskRepository,
                workerContextManager,
                taskMapper
        );
    }

    @Bean
    @Qualifier("impl")
    public TaskCommandStatHelper taskCommandStatHelper(MetricHelper metricHelper) {
        return new TaskCommandStatHelper(
                metricHelper
        );
    }

    @Bean
    @Qualifier("proxy")
    public InternalTaskCommandService internalTaskCommandProxyService(@Qualifier("impl")
                                                                      List<InternalTaskCommandService> internalTaskCommandServices) {
        return new InternalTaskCommandProxyService(internalTaskCommandServices);
    }

    @Bean
    public TaskCommandWithDetectorService localTaskCommandWithDetectorService(WorkerContextManager workerContextManager,
                                                                              PlatformTransactionManager transactionManager,
                                                                              TaskRepository taskRepository,
                                                                              TaskMapper taskMapper,
                                                                              TaskRegistryService taskRegistryService,
                                                                              TaskSerializer taskSerializer,
                                                                              CronService cronService,
                                                                              CommonSettings commonSettings,
                                                                              @Qualifier("proxy") InternalTaskCommandService internalTaskCommandService,
                                                                              TaskLinkManager taskLinkManager,
                                                                              Clock clock) {
        return new LocalTaskCommandServiceImpl(
                workerContextManager,
                transactionManager,
                taskRepository,
                taskMapper,
                taskRegistryService,
                taskSerializer,
                cronService,
                commonSettings,
                internalTaskCommandService,
                taskLinkManager,
                clock
        );
    }

    @Bean
    public TaskCommandWithDetectorService remoteTaskCommandWithDetectorService(WorkerContextManager workerContextManager,
                                                                               PlatformTransactionManager transactionManager,
                                                                               RemoteCommandRepository remoteCommandRepository,
                                                                               TaskSerializer taskSerializer,
                                                                               TaskRegistryService taskRegistryService,
                                                                               Clock clock) {
        return new RemoteTaskCommandServiceImpl(
                workerContextManager,
                transactionManager,
                remoteCommandRepository,
                taskSerializer,
                taskRegistryService,
                clock
        );
    }

    @Bean
    public DistributedTaskService distributedTaskService(TaskRegistryService taskRegistryService,
                                                         List<TaskCommandWithDetectorService> taskCommandServices,
                                                         CommonSettings commonSettings) {
        return new DistributedTaskServiceImpl(
                taskRegistryService,
                taskCommandServices,
                commonSettings
        );
    }

    @Bean
    @Qualifier("localAtLeastOnceWorker")
    public LocalAtLeastOnceWorker localAtLeastOnceWorker(ClusterProvider clusterProvider,
                                                         WorkerContextManager workerContextManager,
                                                         PlatformTransactionManager transactionManager,
                                                         @Qualifier("proxy") InternalTaskCommandService internalTaskCommandService,
                                                         TaskRepository taskRepository,
                                                         RemoteCommandRepository remoteCommandRepository,
                                                         DltRepository dltRepository,
                                                         TaskSerializer taskSerializer,
                                                         CronService cronService,
                                                         TaskMapper taskMapper,
                                                         CommonSettings commonSettings,
                                                         TaskLinkManager taskLinkManager,
                                                         MetricHelper metricHelper,
                                                         Clock clock) {
        return new LocalAtLeastOnceWorker(
                clusterProvider,
                workerContextManager,
                transactionManager,
                internalTaskCommandService,
                taskRepository,
                remoteCommandRepository,
                dltRepository,
                taskSerializer,
                cronService,
                taskMapper,
                commonSettings,
                taskLinkManager,
                metricHelper,
                clock
        );
    }

    @Bean
    @Qualifier("localExactlyOnceWorker")
    public LocalExactlyOnceWorker localExactlyOnceWorker(ClusterProvider clusterProvider,
                                                         WorkerContextManager workerContextManager,
                                                         PlatformTransactionManager transactionManager,
                                                         @Qualifier("proxy") InternalTaskCommandService internalTaskCommandService,
                                                         TaskRepository taskRepository,
                                                         RemoteCommandRepository remoteCommandRepository,
                                                         DltRepository dltRepository,
                                                         TaskSerializer taskSerializer,
                                                         CronService cronService,
                                                         TaskMapper taskMapper,
                                                         CommonSettings commonSettings,
                                                         TaskLinkManager taskLinkManager,
                                                         MetricHelper metricHelper,
                                                         Clock clock) {
        return new LocalExactlyOnceWorker(
                clusterProvider,
                workerContextManager,
                transactionManager,
                internalTaskCommandService,
                taskRepository,
                remoteCommandRepository,
                dltRepository,
                taskSerializer,
                cronService,
                taskMapper,
                commonSettings,
                taskLinkManager,
                metricHelper,
                clock
        );
    }

    @Bean
    public TaskWorkerFactory taskWorkerFactory(List<TaskWorker> taskWorkerList) {
        return new TaskWorkerFactoryImpl(taskWorkerList);
    }

    @Bean
    public WorkerManager workerManager(CommonSettings commonSettings,
                                       ClusterProvider clusterProvider,
                                       TaskRegistryService taskRegistryService,
                                       TaskWorkerFactory taskWorkerFactory,
                                       TaskRepository taskRepository,
                                       TaskMapper taskMapper,
                                       Clock clock,
                                       MetricHelper metricHelper) {
        return new WorkerManagerImpl(
                commonSettings,
                clusterProvider,
                taskRegistryService,
                taskWorkerFactory,
                taskRepository,
                taskMapper,
                clock,
                metricHelper
        );
    }
}
