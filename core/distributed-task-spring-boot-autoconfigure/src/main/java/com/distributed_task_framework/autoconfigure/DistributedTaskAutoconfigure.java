package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.annotation.DtfDataSource;
import com.distributed_task_framework.autoconfigure.mapper.CommonSettingsMerger;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMapper;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMerger;
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
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.impl.ClusterProviderImpl;
import com.distributed_task_framework.service.impl.CompletionServiceImpl;
import com.distributed_task_framework.service.impl.CronService;
import com.distributed_task_framework.service.impl.DeliveryManagerImpl;
import com.distributed_task_framework.service.impl.DistributedTaskServiceImpl;
import com.distributed_task_framework.service.impl.InternalTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.JoinTaskPlannerImpl;
import com.distributed_task_framework.service.impl.JoinTaskStatHelper;
import com.distributed_task_framework.service.impl.JsonTaskSerializerImpl;
import com.distributed_task_framework.service.impl.LocalTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.MetricHelperImpl;
import com.distributed_task_framework.service.impl.PartitionTrackerImpl;
import com.distributed_task_framework.service.impl.RemoteTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.TaskCommandStatServiceImpl;
import com.distributed_task_framework.service.impl.TaskLinkManagerImpl;
import com.distributed_task_framework.service.impl.TaskRegistryServiceImpl;
import com.distributed_task_framework.service.impl.TaskRouter;
import com.distributed_task_framework.service.impl.TaskWorkerFactoryImpl;
import com.distributed_task_framework.service.impl.VirtualQueueBaseFairTaskPlannerImpl;
import com.distributed_task_framework.service.impl.VirtualQueueBaseTaskCommandServiceImpl;
import com.distributed_task_framework.service.impl.VirtualQueueManagerPlannerImpl;
import com.distributed_task_framework.service.impl.VirtualQueueStatHelper;
import com.distributed_task_framework.service.impl.WorkerContextManagerImpl;
import com.distributed_task_framework.service.impl.WorkerManagerImpl;
import com.distributed_task_framework.service.impl.workers.LocalAtLeastOnceWorker;
import com.distributed_task_framework.service.impl.workers.LocalExactlyOnceWorker;
import com.distributed_task_framework.service.internal.CapabilityRegister;
import com.distributed_task_framework.service.internal.CapabilityRegisterProvider;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.CompletionService;
import com.distributed_task_framework.service.internal.DeliveryManager;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.service.internal.PlannerService;
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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.sun.management.OperatingSystemMXBean;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.mapstruct.factory.Mappers;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.distributed_task_framework.autoconfigure.TaskConfigurationDiscoveryProcessor.EMPTY_TASK_SETTINGS_CUSTOMIZER;
import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_TX_MANAGER;

@Slf4j
@Configuration
@ConditionalOnClass(DistributedTaskService.class)
@EnableConfigurationProperties({
    DistributedTaskProperties.class
})
@ConditionalOnProperty(name = "distributed-task.enabled", havingValue = "true")
@AutoConfigureAfter(
    value = {
        JdbcTemplateAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class
    }
)
@EnableJdbcRepositories(
    basePackageClasses = NodeStateRepository.class,
    transactionManagerRef = DTF_TX_MANAGER,
    jdbcOperationsRef = DTF_JDBC_OPS
)
@EnableTransactionManagement
@EnableCaching
@ComponentScan(basePackageClasses = CommonSettingsMerger.class)
public class DistributedTaskAutoconfigure {

    @Bean
    @ConditionalOnMissingBean
    public Clock distributedTaskInternalClock() {
        return Clock.systemUTC();
    }

    @Bean("dtfDataSource")
    @Conditional(DtfDataSourceCondition.class)
    @DtfDataSource
    public DataSource dtfDataSource(DataSource defaultDataSource) {
        return defaultDataSource;
    }

    @ConditionalOnMissingBean(name = DTF_TX_MANAGER)
    @Bean
    public DataSourceTransactionManager dtfTransactionManager(@DtfDataSource DataSource dtfDataSource) {
        return new DataSourceTransactionManager(dtfDataSource);
    }

    @ConditionalOnMissingBean(name = DTF_JDBC_OPS)
    @Bean
    public NamedParameterJdbcOperations dtfNamedParameterJdbcOperations(@DtfDataSource DataSource dtfDataSource) {
        return new NamedParameterJdbcTemplate(dtfDataSource);
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
    @ConditionalOnMissingBean
    @Qualifier("commonRegistryCaffeineConfig")
    public Caffeine<Object, Object> commonRegistryCaffeineConfig(CommonSettings commonSettings) {
        return Caffeine.newBuilder()
            .expireAfterWrite(commonSettings.getRegistrySettings().getCacheExpirationMs(), TimeUnit.MILLISECONDS);
    }

    @Bean
    @ConditionalOnMissingBean
    @Qualifier("commonRegistryCacheManager")
    public CacheManager commonRegistryCacheManager(
        @Qualifier("commonRegistryCaffeineConfig") Caffeine<Object, Object> caffeine) {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeine(caffeine);
        return caffeineCacheManager;
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
    @ConditionalOnBean(name = DTF_TX_MANAGER)
    public ClusterProvider clusterProvider(CommonSettings commonSettings,
                                           @Lazy CapabilityRegisterProvider capabilityRegisterProvider,
                                           @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                           NodeStateMapper nodeStateMapper,
                                           CacheManager cacheManager,
                                           NodeStateRepository nodeStateRepository,
                                           CapabilityRepository capabilityRepository,
                                           OperatingSystemMXBeanHolder operatingSystemMXBeanHolder,
                                           Clock clock) {
        return new ClusterProviderImpl(
            commonSettings,
            capabilityRegisterProvider,
            transactionManager,
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
    @ConditionalOnBean(name = DTF_TX_MANAGER)
    public TaskRegistryService taskRegistryService(CommonSettings commonSettings,
                                                   RegisteredTaskRepository registeredTaskRepository,
                                                   @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                                   ClusterProvider clusterProvider) {
        return new TaskRegistryServiceImpl(
            commonSettings,
            registeredTaskRepository,
            transactionManager,
            clusterProvider
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
    public MetricHelper metricHelper(MeterRegistry meterRegistry) {
        return new MetricHelperImpl(meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
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
    @ConditionalOnMissingBean
    public JoinTaskStatHelper joinTaskStatHelper(MetricHelper metricHelper) {
        return new JoinTaskStatHelper(metricHelper);
    }

    @Bean
    @Qualifier("virtualQueueManagerPlanner")
    @ConditionalOnMissingBean
    public VirtualQueueManagerPlannerImpl virtualQueueManagerPlanner(CommonSettings commonSettings,
                                                                     PlannerRepository plannerRepository,
                                                                     @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                                                     ClusterProvider clusterProvider,
                                                                     TaskRepository taskRepository,
                                                                     PartitionTracker partitionTracker,
                                                                     TaskMapper taskMapper,
                                                                     VirtualQueueStatHelper virtualQueueStatHelper,
                                                                     MetricHelper metricHelper) {
        return new VirtualQueueManagerPlannerImpl(
            commonSettings,
            plannerRepository,
            transactionManager,
            clusterProvider,
            taskRepository,
            partitionTracker,
            taskMapper,
            virtualQueueStatHelper,
            metricHelper
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskRouter taskRouter() {
        return new TaskRouter();
    }

    @Bean
    @Qualifier("virtualQueueBaseFairTaskPlanner")
    @ConditionalOnMissingBean
    public VirtualQueueBaseFairTaskPlannerImpl virtualQueueBaseFairTaskPlanner(CommonSettings commonSettings,
                                                                               PlannerRepository plannerRepository,
                                                                               @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                                                               ClusterProvider clusterProvider,
                                                                               TaskRepository taskRepository,
                                                                               PartitionTracker partitionTracker,
                                                                               TaskRegistryService taskRegistryService,
                                                                               TaskRouter taskRouter,
                                                                               VirtualQueueStatHelper virtualQueueStatHelper,
                                                                               Clock clock,
                                                                               MetricHelper metricHelper) {
        return new VirtualQueueBaseFairTaskPlannerImpl(
            commonSettings,
            plannerRepository,
            transactionManager,
            clusterProvider,
            taskRepository,
            partitionTracker,
            taskRegistryService,
            taskRouter,
            virtualQueueStatHelper,
            clock,
            metricHelper
        );
    }

    @Bean
    @Qualifier("joinTaskPlannerService")
    @ConditionalOnMissingBean
    public JoinTaskPlannerImpl joinTaskPlannerService(CommonSettings commonSettings,
                                                      PlannerRepository plannerRepository,
                                                      @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                                      ClusterProvider clusterProvider,
                                                      TaskLinkManager taskLinkManager,
                                                      TaskRepository taskRepository,
                                                      MetricHelper metricHelper,
                                                      JoinTaskStatHelper statHelper,
                                                      Clock clock) {
        return new JoinTaskPlannerImpl(
            commonSettings,
            plannerRepository,
            transactionManager,
            clusterProvider,
            taskLinkManager,
            taskRepository,
            metricHelper,
            statHelper,
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
    public PartitionTracker partitionTracker(@Qualifier(DTF_TX_MANAGER) PlatformTransactionManager platformTransactionManager,
                                             TaskRepository taskRepository,
                                             PartitionRepository partitionRepository,
                                             PartitionMapper partitionMapper,
                                             CommonSettings commonSettings,
                                             Clock clock) {
        return new PartitionTrackerImpl(
            platformTransactionManager,
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
    public TaskCommandStatService taskCommandStatService(MetricHelper metricHelper) {
        return new TaskCommandStatServiceImpl(metricHelper);
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
                                                                              @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
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
            transactionManager,
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
                                                                             @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
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
                                                         @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                                         InternalTaskCommandService internalTaskCommandService,
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
    @ConditionalOnMissingBean
    public LocalExactlyOnceWorker localExactlyOnceWorker(ClusterProvider clusterProvider,
                                                         WorkerContextManager workerContextManager,
                                                         @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                                         InternalTaskCommandService internalTaskCommandService,
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
                                           @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                           Clock clock) {
        return new DeliveryManagerImpl(
            commonSettings,
            remoteTaskWorkerRepository,
            remoteCommandRepository,
            dlcRepository,
            clusterProvider,
            commandMapper,
            taskSerializer,
            transactionManager,
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
