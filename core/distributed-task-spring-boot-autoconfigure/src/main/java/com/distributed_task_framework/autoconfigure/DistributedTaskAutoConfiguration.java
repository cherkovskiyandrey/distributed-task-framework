package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.mapper.CommonSettingsMerger;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMapper;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMerger;
import com.distributed_task_framework.mapper.CommandMapper;
import com.distributed_task_framework.mapper.IdVersionMapper;
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
import com.distributed_task_framework.persistence.repository.jdbc.TaskRepositoryHelper;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.PlannerState;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.impl.ClusterProviderImpl;
import com.distributed_task_framework.service.impl.CompletionServiceImpl;
import com.distributed_task_framework.service.impl.CronService;
import com.distributed_task_framework.service.impl.DeliveryManagerImpl;
import com.distributed_task_framework.service.impl.DistributedTaskMetricHelperImpl;
import com.distributed_task_framework.service.impl.DistributedTaskServiceImpl;
import com.distributed_task_framework.service.impl.InternalTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.JoinTaskPlannerImpl;
import com.distributed_task_framework.service.impl.JoinTaskStatHelper;
import com.distributed_task_framework.service.impl.JsonTaskSerializerImpl;
import com.distributed_task_framework.service.impl.LocalTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.PartitionTrackerImpl;
import com.distributed_task_framework.service.impl.PlannerStateImpl;
import com.distributed_task_framework.service.impl.RemoteTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.TaskCommandStatServiceImpl;
import com.distributed_task_framework.service.impl.TaskLinkManagerImpl;
import com.distributed_task_framework.service.impl.TaskRegistryServiceImpl;
import com.distributed_task_framework.service.impl.TaskRouter;
import com.distributed_task_framework.service.impl.TaskWorkerFactoryImpl;
import com.distributed_task_framework.service.impl.VirtualQueueBaseFairTaskPlannerImpl;
import com.distributed_task_framework.service.impl.VirtualQueueBaseTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.VirtualQueueManagerPlannerImpl;
import com.distributed_task_framework.service.impl.VirtualQueueStatService;
import com.distributed_task_framework.service.impl.WorkerContextManagerImpl;
import com.distributed_task_framework.service.impl.WorkerManagerImpl;
import com.distributed_task_framework.service.impl.workers.LocalAtLeastOnceWorker;
import com.distributed_task_framework.service.impl.workers.LocalExactlyOnceWorker;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.CompletionService;
import com.distributed_task_framework.service.internal.DeliveryManager;
import com.distributed_task_framework.service.internal.DistributedTaskMetricHelper;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.service.internal.PlannerStateRegistry;
import com.distributed_task_framework.service.internal.TaskCommandStatService;
import com.distributed_task_framework.service.internal.TaskCommandWithDetectorService;
import com.distributed_task_framework.service.internal.TaskLinkManager;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.service.internal.TaskWorkerFactory;
import com.distributed_task_framework.service.internal.VirtualQueueBaseTaskCommandService;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.utils.CaffeineDistributedTaskCacheManagerImpl;
import com.distributed_task_framework.utils.DistributedTaskCacheManager;
import com.distributed_task_framework.utils.DistributedTaskLifecycleSpringConfiguration;
import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import com.sun.management.OperatingSystemMXBean;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.mapstruct.factory.Mappers;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;

import java.lang.management.ManagementFactory;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.distributed_task_framework.autoconfigure.TaskConfigurationDiscoveryProcessor.EMPTY_TASK_SETTINGS_CUSTOMIZER;

@Slf4j
@AutoConfiguration
@ConditionalOnClass(DistributedTaskService.class)
@EnableConfigurationProperties(DistributedTaskProperties.class)
@ConditionalOnProperty(name = "distributed-task.enabled", havingValue = "true")
@AutoConfigureAfter(
    value = {
        DistributedTaskSpringInfrastructureAutoconfiguration.class
    }
)
@EnableJdbcRepositories(
    basePackageClasses = NodeStateRepository.class,
    repositoryFactoryBeanClass = DtfJdbcRepositoryFactoryBean.class
)
@Import(DistributedTaskLifecycleSpringConfiguration.class)
@ComponentScan(basePackageClasses = CommonSettingsMerger.class)
public class DistributedTaskAutoConfiguration {
    private static final String INTERNAL_DISTRIBUTED_TASK_CACHE_MANAGER_NAME = "internalDistributedTaskCacheManager";

    public static final String VIRTUAL_QUEUE_MANAGER_PLANNER_NAME = "virtualQueueManagerPlanner";
    public static final String VIRTUAL_QUEUE_BASE_FAIR_TASK_PLANNER_NAME = "virtualQueueBaseFairTaskPlanner";
    public static final String JOIN_TASK_PLANNER_SERVICE_NAME = "joinTaskPlannerService";

    @Bean
    @ConditionalOnMissingBean
    public Clock distributedTaskInternalClock() {
        return Clock.systemUTC();
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

    @Bean(INTERNAL_DISTRIBUTED_TASK_CACHE_MANAGER_NAME)
    @ConditionalOnMissingBean(name = INTERNAL_DISTRIBUTED_TASK_CACHE_MANAGER_NAME)
    @Qualifier(INTERNAL_DISTRIBUTED_TASK_CACHE_MANAGER_NAME)
    public DistributedTaskCacheManager internalDistributedTaskCacheManager() {
        return new CaffeineDistributedTaskCacheManagerImpl();
    }

    @Bean
    @ConditionalOnMissingBean
    public CronService cronService(Clock clock) {
        return new CronService(clock);
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskMapper taskMapper() {
        return Mappers.getMapper(TaskMapper.class);
    }

    @Bean
    @ConditionalOnMissingBean
    public NodeStateMapper nodeStateMapper() {
        return Mappers.getMapper(NodeStateMapper.class);
    }

    @Bean
    @ConditionalOnMissingBean
    public CommandMapper commandMapper() {
        return Mappers.getMapper(CommandMapper.class);
    }

    @Bean
    @ConditionalOnMissingBean
    public IdVersionMapper idVersionMapper() {
        return Mappers.getMapper(IdVersionMapper.class);
    }

    @Bean
    @ConditionalOnMissingBean
    public PartitionMapper partitionMapper() {
        return Mappers.getMapper(PartitionMapper.class);
    }

    @Bean
    @ConditionalOnMissingBean
    public CommonSettings commonSettings(CommonSettingsMerger commonSettingsMerger,
                                         DistributedTaskProperties properties) {
        return commonSettingsMerger.merge(
            CommonSettings.DEFAULT.toBuilder().build(),
            properties.getCommon()
        );
    }

    //use in order to escape conflict with beans form other standard libraries like spring-boot-starter-actuator
    //because simple using of conditional doesn't work
    public record OperatingSystemMXBeanHolder(OperatingSystemMXBean operatingSystemMXBean) {
    }

    @Bean
    @ConditionalOnMissingBean
    public OperatingSystemMXBeanHolder dtfOperatingSystemMXBean() {
        return new OperatingSystemMXBeanHolder((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean());
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskRepositoryHelper taskRepositoryHelper(DtfJdbcInfrastructure dtfJdbcInfrastructure) {
        return new TaskRepositoryHelper(dtfJdbcInfrastructure);
    }

    @Bean
    @ConditionalOnMissingBean
    public ClusterProvider clusterProvider(CommonSettings commonSettings,
                                           DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                           @Qualifier(INTERNAL_DISTRIBUTED_TASK_CACHE_MANAGER_NAME) DistributedTaskCacheManager cacheManager,
                                           NodeStateMapper nodeStateMapper,
                                           NodeStateRepository nodeStateRepository,
                                           CapabilityRepository capabilityRepository,
                                           OperatingSystemMXBeanHolder operatingSystemMXBeanHolder,
                                           Clock clock) {
        return new ClusterProviderImpl(
            commonSettings,
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            cacheManager,
            nodeStateMapper,
            nodeStateRepository,
            capabilityRepository,
            operatingSystemMXBeanHolder.operatingSystemMXBean(),
            clock
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskRegistryService taskRegistryService(CommonSettings commonSettings,
                                                   RegisteredTaskRepository registeredTaskRepository,
                                                   DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                                   @Qualifier(INTERNAL_DISTRIBUTED_TASK_CACHE_MANAGER_NAME) DistributedTaskCacheManager distributedTaskCacheManager,
                                                   ClusterProvider clusterProvider,
                                                   CronService cronService) {
        return new TaskRegistryServiceImpl(
            commonSettings,
            registeredTaskRepository,
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            distributedTaskCacheManager,
            clusterProvider,
            cronService
        );
    }


    private ObjectMapper createObjectMapper() {
        var objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        //in order to easily add new properties to the task message and be tolerant during rolling out
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        objectMapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);
        objectMapper.configure(SerializationFeature.WRITE_SELF_REFERENCES_AS_NULL, true);
        objectMapper.configure(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, false);
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.findAndRegisterModules();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new KotlinModule.Builder().build());
        return objectMapper;
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskSerializer taskSerializer() {
        return new JsonTaskSerializerImpl(createObjectMapper());
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskLinkManager taskLinkManager(TaskRepository taskRepository,
                                           TaskLinkRepository taskLinkRepository,
                                           TaskMessageRepository taskMessageRepository,
                                           CommonSettings commonSettings,
                                           TaskSerializer taskSerializer,
                                           TaskMapper taskMapper) {
        return new TaskLinkManagerImpl(
            taskRepository,
            taskLinkRepository,
            taskMessageRepository,
            commonSettings,
            taskSerializer,
            taskMapper
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public DistributedTaskMetricHelper metricHelper(MeterRegistry meterRegistry) {
        return new DistributedTaskMetricHelperImpl(meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean({PlannerState.class, PlannerStateRegistry.class})
    public PlannerStateImpl plannerStateRegistry() {
        return new PlannerStateImpl();
    }

    @Bean
    @ConditionalOnMissingBean
    public VirtualQueueStatService virtualQueueStatHelper(PlannerState plannerState,
                                                          CommonSettings commonSettings,
                                                          TaskRegistryService taskRegistryService,
                                                          TaskRepository taskRepository,
                                                          TaskMapper taskMapper,
                                                          DistributedTaskMetricHelper distributedTaskMetricHelper,
                                                          MeterRegistry meterRegistry) {
        return new VirtualQueueStatService(
            plannerState,
            commonSettings,
            taskRegistryService,
            taskRepository,
            taskMapper,
            distributedTaskMetricHelper,
            meterRegistry
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public JoinTaskStatHelper joinTaskStatHelper(DistributedTaskMetricHelper distributedTaskMetricHelper) {
        return new JoinTaskStatHelper(distributedTaskMetricHelper);
    }

    @Bean
    @Qualifier(VIRTUAL_QUEUE_MANAGER_PLANNER_NAME)
    @ConditionalOnMissingBean(name = VIRTUAL_QUEUE_MANAGER_PLANNER_NAME)
    public VirtualQueueManagerPlannerImpl virtualQueueManagerPlanner(CommonSettings commonSettings,
                                                                     PlannerRepository plannerRepository,
                                                                     DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                                                     ClusterProvider clusterProvider,
                                                                     TaskRepository taskRepository,
                                                                     PartitionTracker partitionTracker,
                                                                     TaskMapper taskMapper,
                                                                     PlannerStateRegistry plannerStateRegistry,
                                                                     IdVersionMapper idVersionMapper,
                                                                     VirtualQueueStatService virtualQueueStatService,
                                                                     DistributedTaskMetricHelper distributedTaskMetricHelper) {
        return new VirtualQueueManagerPlannerImpl(
            commonSettings,
            plannerRepository,
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            clusterProvider,
            taskRepository,
            partitionTracker,
            taskMapper,
            idVersionMapper,
            virtualQueueStatService,
            plannerStateRegistry,
            distributedTaskMetricHelper
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskRouter taskRouter() {
        return new TaskRouter();
    }

    @Bean
    @Qualifier(VIRTUAL_QUEUE_BASE_FAIR_TASK_PLANNER_NAME)
    @ConditionalOnMissingBean(name = VIRTUAL_QUEUE_BASE_FAIR_TASK_PLANNER_NAME)
    public VirtualQueueBaseFairTaskPlannerImpl virtualQueueBaseFairTaskPlanner(CommonSettings commonSettings,
                                                                               PlannerRepository plannerRepository,
                                                                               DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                                                               ClusterProvider clusterProvider,
                                                                               TaskRepository taskRepository,
                                                                               PartitionTracker partitionTracker,
                                                                               TaskRegistryService taskRegistryService,
                                                                               TaskRouter taskRouter,
                                                                               PlannerStateRegistry plannerStateRegistry,
                                                                               VirtualQueueStatService virtualQueueStatService,
                                                                               Clock clock,
                                                                               DistributedTaskMetricHelper distributedTaskMetricHelper) {
        return new VirtualQueueBaseFairTaskPlannerImpl(
            commonSettings,
            plannerRepository,
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            clusterProvider,
            taskRepository,
            partitionTracker,
            taskRegistryService,
            taskRouter,
            virtualQueueStatService,
            clock,
            plannerStateRegistry,
            distributedTaskMetricHelper
        );
    }

    @Bean
    @Qualifier(JOIN_TASK_PLANNER_SERVICE_NAME)
    @ConditionalOnMissingBean(name = JOIN_TASK_PLANNER_SERVICE_NAME)
    public JoinTaskPlannerImpl joinTaskPlannerService(CommonSettings commonSettings,
                                                      PlannerRepository plannerRepository,
                                                      DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                                      ClusterProvider clusterProvider,
                                                      TaskLinkManager taskLinkManager,
                                                      TaskRepository taskRepository,
                                                      DistributedTaskMetricHelper distributedTaskMetricHelper,
                                                      JoinTaskStatHelper statHelper,
                                                      PlannerStateRegistry plannerStateRegistry,
                                                      Clock clock) {
        return new JoinTaskPlannerImpl(
            commonSettings,
            plannerRepository,
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            clusterProvider,
            taskLinkManager,
            taskRepository,
            distributedTaskMetricHelper,
            statHelper,
            plannerStateRegistry,
            clock
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkerContextManager workerContextManager() {
        return new WorkerContextManagerImpl();
    }

    @Bean
    @ConditionalOnMissingBean
    public PartitionTracker partitionTracker(DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                             TaskRepository taskRepository,
                                             PartitionRepository partitionRepository,
                                             PartitionMapper partitionMapper,
                                             CommonSettings commonSettings,
                                             Clock clock) {
        return new PartitionTrackerImpl(
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            taskRepository,
            partitionRepository,
            partitionMapper,
            commonSettings,
            clock
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public VirtualQueueBaseTaskCommandService virtualQueueBaseTaskCommandService(PartitionTracker partitionTracker,
                                                                                 TaskRepository taskRepository,
                                                                                 WorkerContextManager workerContextManager,
                                                                                 TaskMapper taskMapper) {
        return new VirtualQueueBaseTaskCommandServiceImpl(
            partitionTracker,
            taskRepository,
            workerContextManager,
            taskMapper
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskCommandStatService taskCommandStatService(DistributedTaskMetricHelper distributedTaskMetricHelper) {
        return new TaskCommandStatServiceImpl(distributedTaskMetricHelper);
    }

    @Bean
    @ConditionalOnMissingBean
    public InternalTaskCommandService internalTaskCommandService(VirtualQueueBaseTaskCommandService internalTaskCommandServices,
                                                                 TaskCommandStatService taskCommandStatService) {
        return new InternalTaskCommandServiceImpl(
            internalTaskCommandServices,
            taskCommandStatService
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public CompletionService completionService(CommonSettings commonSettings,
                                               TaskRepository taskRepository,
                                               WorkerContextManager workerContextManager) {
        return new CompletionServiceImpl(
            commonSettings,
            taskRepository,
            workerContextManager
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskCommandWithDetectorService localTaskCommandWithDetectorService(WorkerContextManager workerContextManager,
                                                                              DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                                                              TaskRepository taskRepository,
                                                                              TaskMapper taskMapper,
                                                                              TaskRegistryService taskRegistryService,
                                                                              TaskSerializer taskSerializer,
                                                                              CronService cronService,
                                                                              CommonSettings commonSettings,
                                                                              TaskLinkManager taskLinkManager,
                                                                              InternalTaskCommandService internalTaskCommandService,
                                                                              CompletionService completionService,
                                                                              Clock clock) {
        return new LocalTaskCommandServiceImpl(
            workerContextManager,
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            taskRepository,
            taskMapper,
            taskRegistryService,
            taskSerializer,
            cronService,
            commonSettings,
            internalTaskCommandService,
            taskLinkManager,
            completionService,
            clock
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public RemoteTaskCommandServiceImpl remoteTaskCommandWithDetectorService(WorkerContextManager workerContextManager,
                                                                             DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                                                             RemoteCommandRepository remoteCommandRepository,
                                                                             TaskSerializer taskSerializer,
                                                                             TaskRegistryService taskRegistryService,
                                                                             Clock clock) {
        return new RemoteTaskCommandServiceImpl(
            workerContextManager,
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            remoteCommandRepository,
            taskSerializer,
            taskRegistryService,
            clock
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public DistributedTaskServiceImpl distributedTaskService(TaskRegistryService taskRegistryService,
                                                             List<TaskCommandWithDetectorService> taskCommandServices,
                                                             CommonSettings commonSettings) {
        return new DistributedTaskServiceImpl(
            taskRegistryService,
            taskCommandServices,
            commonSettings
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public RemoteTasks remoteTasks() {
        return new RemoteTasks() {
        };
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DistributedTaskService.class)
    public TaskConfigurationDiscoveryProcessor taskConfigurationDiscoveryProcessor(DistributedTaskProperties properties,
                                                                                   DistributedTaskService distributedTaskService,
                                                                                   DistributedTaskPropertiesMapper distributedTaskPropertiesMapper,
                                                                                   DistributedTaskPropertiesMerger distributedTaskPropertiesMerger,
                                                                                   Collection<Task<?>> tasks,
                                                                                   RemoteTasks remoteTasks) {
        return new TaskConfigurationDiscoveryProcessor(
            properties,
            distributedTaskService,
            distributedTaskPropertiesMapper,
            distributedTaskPropertiesMerger,
            tasks,
            remoteTasks,
            EMPTY_TASK_SETTINGS_CUSTOMIZER
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public LocalAtLeastOnceWorker localAtLeastOnceWorker(ClusterProvider clusterProvider,
                                                         WorkerContextManager workerContextManager,
                                                         DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                                         InternalTaskCommandService internalTaskCommandService,
                                                         TaskRepository taskRepository,
                                                         RemoteCommandRepository remoteCommandRepository,
                                                         DltRepository dltRepository,
                                                         TaskSerializer taskSerializer,
                                                         CronService cronService,
                                                         TaskMapper taskMapper,
                                                         CommonSettings commonSettings,
                                                         TaskLinkManager taskLinkManager,
                                                         DistributedTaskMetricHelper distributedTaskMetricHelper,
                                                         Clock clock) {
        return new LocalAtLeastOnceWorker(
            clusterProvider,
            workerContextManager,
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            internalTaskCommandService,
            taskRepository,
            remoteCommandRepository,
            dltRepository,
            taskSerializer,
            cronService,
            taskMapper,
            commonSettings,
            taskLinkManager,
            distributedTaskMetricHelper,
            clock
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public LocalExactlyOnceWorker localExactlyOnceWorker(ClusterProvider clusterProvider,
                                                         WorkerContextManager workerContextManager,
                                                         DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                                         InternalTaskCommandService internalTaskCommandService,
                                                         TaskRepository taskRepository,
                                                         RemoteCommandRepository remoteCommandRepository,
                                                         DltRepository dltRepository,
                                                         TaskSerializer taskSerializer,
                                                         CronService cronService,
                                                         TaskMapper taskMapper,
                                                         CommonSettings commonSettings,
                                                         TaskLinkManager taskLinkManager,
                                                         DistributedTaskMetricHelper distributedTaskMetricHelper,
                                                         Clock clock) {
        return new LocalExactlyOnceWorker(
            clusterProvider,
            workerContextManager,
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            internalTaskCommandService,
            taskRepository,
            remoteCommandRepository,
            dltRepository,
            taskSerializer,
            cronService,
            taskMapper,
            commonSettings,
            taskLinkManager,
            distributedTaskMetricHelper,
            clock
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskWorkerFactory taskWorkerFactory(List<TaskWorker> taskWorkerList) {
        return new TaskWorkerFactoryImpl(taskWorkerList);
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkerManager workerManager(CommonSettings commonSettings,
                                       ClusterProvider clusterProvider,
                                       TaskRegistryService taskRegistryService,
                                       TaskWorkerFactory taskWorkerFactory,
                                       TaskRepository taskRepository,
                                       TaskMapper taskMapper,
                                       Clock clock,
                                       DistributedTaskMetricHelper distributedTaskMetricHelper) {
        return new WorkerManagerImpl(
            commonSettings,
            clusterProvider,
            taskRegistryService,
            taskWorkerFactory,
            taskRepository,
            taskMapper,
            clock,
            distributedTaskMetricHelper
        );
    }

    @Bean
    @Conditional(DeliveryManagerCondition.class)
    @ConditionalOnMissingBean
    public DeliveryManager deliveryManager(CommonSettings commonSettings,
                                           RemoteTaskWorkerRepository remoteTaskWorkerRepository,
                                           RemoteCommandRepository remoteCommandRepository,
                                           DlcRepository dlcRepository,
                                           ClusterProvider clusterProvider,
                                           CommandMapper commandMapper,
                                           TaskSerializer taskSerializer,
                                           DtfJdbcInfrastructure dtfJdbcInfrastructure,
                                           Clock clock) {
        return new DeliveryManagerImpl(
            commonSettings,
            remoteTaskWorkerRepository,
            remoteCommandRepository,
            dlcRepository,
            clusterProvider,
            commandMapper,
            taskSerializer,
            dtfJdbcInfrastructure.getPlatformTransactionManager(),
            clock
        );
    }

    public static class DeliveryManagerCondition implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            MutablePropertySources propertySources = ((AbstractEnvironment) context.getEnvironment()).getPropertySources();
            long numOfRemoteApp = propertySources.stream()
                .filter(propertySource -> propertySource instanceof MapPropertySource)
                .map(propertySource -> (MapPropertySource) propertySource)
                .flatMap(propertySource -> Arrays.stream(propertySource.getPropertyNames()))
                .filter(propName -> propName.startsWith(
                    "distributed-task.common.delivery-manager.remote-apps.app-to-url."))
                .count();
            return numOfRemoteApp > 0;
        }
    }
}
