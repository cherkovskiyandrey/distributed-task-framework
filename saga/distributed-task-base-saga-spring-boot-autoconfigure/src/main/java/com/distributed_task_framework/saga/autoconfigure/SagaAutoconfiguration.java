package com.distributed_task_framework.saga.autoconfigure;


import com.distributed_task_framework.autoconfigure.DistributedTaskAutoconfigure;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaCommonPropertiesMapper;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaCommonPropertiesMerger;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaMethodPropertiesMapper;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaMethodPropertiesMerger;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaPropertiesMapper;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaPropertiesMerger;
import com.distributed_task_framework.saga.autoconfigure.services.SagaPropertiesProcessor;
import com.distributed_task_framework.saga.autoconfigure.services.impl.SagaPropertiesProcessorImpl;
import com.distributed_task_framework.saga.mappers.SagaMapper;
import com.distributed_task_framework.saga.mappers.SettingsMapper;
import com.distributed_task_framework.saga.persistence.repository.DlsSagaContextRepository;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.services.SagaFlowEntryPoint;
import com.distributed_task_framework.saga.services.SagaRegisterService;
import com.distributed_task_framework.saga.services.impl.DistributionSagaServiceImpl;
import com.distributed_task_framework.saga.services.impl.SagaHelper;
import com.distributed_task_framework.saga.services.impl.SagaManagerImpl;
import com.distributed_task_framework.saga.services.impl.SagaRegisterServiceImpl;
import com.distributed_task_framework.saga.services.impl.SagaResolverImpl;
import com.distributed_task_framework.saga.services.impl.SagaTaskFactoryImpl;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.saga.settings.SagaCommonSettings;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.mapstruct.factory.Mappers;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_TX_MANAGER;

@Configuration
@ConditionalOnClass(SagaFlowEntryPoint.class)
@ConditionalOnProperty(
    prefix = "distributed-task",
    name = {
        "enabled",
        "saga.enabled"
    },
    havingValue = "true"
)
@AutoConfigureAfter(
    DistributedTaskAutoconfigure.class
)
@EnableJdbcRepositories(
    basePackageClasses = SagaRepository.class,
    transactionManagerRef = DTF_TX_MANAGER,
    jdbcOperationsRef = DTF_JDBC_OPS
)
@EnableTransactionManagement
@EnableConfigurationProperties(value = DistributedSagaProperties.class)
@ComponentScan(basePackageClasses = SagaMethodPropertiesMapper.class)
@EnableCaching
public class SagaAutoconfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Clock sagaInternalClock() {
        return Clock.systemUTC();
    }

    @Bean
    @Qualifier("commonSagaCaffeineConfig")
    @ConditionalOnMissingBean
    public Caffeine<Object, Object> commonSagaCaffeineConfig() {
        return Caffeine.newBuilder().expireAfterWrite(0, TimeUnit.MILLISECONDS);
    }

    @Bean
    @Qualifier("commonSagaCacheManager")
    @ConditionalOnMissingBean
    public CacheManager commonSagaCacheManager(@Qualifier("commonSagaCaffeineConfig") Caffeine<Object, Object> caffeine) {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeine(caffeine);
        return caffeineCacheManager;
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaPropertiesProcessor sagaPropertiesProcessor(SagaCommonPropertiesMapper sagaCommonPropertiesMapper,
                                                           SagaCommonPropertiesMerger sagaCommonPropertiesMerger,
                                                           SagaPropertiesMapper sagaPropertiesMapper,
                                                           SagaPropertiesMerger sagaPropertiesMerger,
                                                           SagaMethodPropertiesMapper sagaMethodPropertiesMapper,
                                                           SagaMethodPropertiesMerger sagaMethodPropertiesMerger) {
        return new SagaPropertiesProcessorImpl(
            sagaCommonPropertiesMapper,
            sagaCommonPropertiesMerger,
            sagaPropertiesMapper,
            sagaPropertiesMerger,
            sagaMethodPropertiesMapper,
            sagaMethodPropertiesMerger
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaCommonSettings sagaCommonSettings(SagaPropertiesProcessor sagaPropertiesProcessor,
                                                 DistributedSagaProperties distributedSagaProperties) {
        return sagaPropertiesProcessor.buildSagaCommonSettings(distributedSagaProperties.getCommon());
    }

    @Bean
    @ConditionalOnMissingBean
    public SettingsMapper settingsMapper() {
        return Mappers.getMapper(SettingsMapper.class);
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaMapper sagaMapper() {
        return Mappers.getMapper(SagaMapper.class);
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaHelper sagaHelper(TaskSerializer taskSerializer) {
        return new SagaHelper(taskSerializer);
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaResolver sagaResolver(TaskRegistryService taskRegistryService) {
        return new SagaResolverImpl(taskRegistryService);
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaManager sagaContextService(DistributedTaskService distributedTaskService,
                                          SagaRepository sagaRepository,
                                          DlsSagaContextRepository dlsSagaContextRepository,
                                          SagaHelper sagaHelper,
                                          SagaMapper sagaMapper,
                                          @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                          Clock clock,
                                          SagaCommonSettings sagaCommonSettings) {
        return new SagaManagerImpl(
            distributedTaskService,
            sagaRepository,
            dlsSagaContextRepository,
            sagaHelper,
            sagaMapper,
            transactionManager,
            sagaCommonSettings,
            clock
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaTaskFactory sagaTaskFactory(SagaResolver sagaResolver,
                                           DistributedTaskService distributedTaskService,
                                           TaskSerializer taskSerializer,
                                           SagaManager sagaManager,
                                           SagaHelper sagaHelper) {
        return new SagaTaskFactoryImpl(
            sagaResolver,
            distributedTaskService,
            sagaManager,
            taskSerializer,
            sagaHelper
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaRegisterService sagaRegisterService(DistributedTaskService distributedTaskService,
                                                   SagaTaskFactory sagaTaskFactory,
                                                   SagaResolver sagaResolver,
                                                   SettingsMapper settingsMapper) {
        return new SagaRegisterServiceImpl(
            distributedTaskService,
            sagaTaskFactory,
            sagaResolver,
            settingsMapper
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public DistributionSagaService distributionSagaService(@Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                                           SagaResolver sagaResolver,
                                                           SagaRegisterService sagaRegisterService,
                                                           DistributedTaskService distributedTaskService,
                                                           SagaManager sagaManager,
                                                           SagaHelper sagaHelper) {
        return new DistributionSagaServiceImpl(
            transactionManager,
            sagaResolver,
            sagaRegisterService,
            distributedTaskService,
            sagaManager,
            sagaHelper
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaConfigurationDiscoveryProcessor sagaConfigurationDiscoveryProcessor(DistributionSagaService distributionSagaService,
                                                                                   DistributedSagaProperties distributedSagaProperties,
                                                                                   SagaPropertiesProcessor sagaPropertiesProcessor) {
        return new SagaConfigurationDiscoveryProcessor(
            distributionSagaService,
            distributedSagaProperties,
            sagaPropertiesProcessor
        );
    }
}
