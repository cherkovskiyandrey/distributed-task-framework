package com.distributed_task_framework.saga;


import com.distributed_task_framework.autoconfigure.DistributedTaskAutoconfigure;
import com.distributed_task_framework.saga.mappers.ContextMapper;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.SagaFlowEntryPoint;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.Clock;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_TX_MANAGER;

@Configuration
@ConditionalOnClass(SagaFlowEntryPoint.class)
@ConditionalOnProperty(name = "distributed-task.enabled", havingValue = "true")
@AutoConfigureAfter(
    DistributedTaskAutoconfigure.class
)
@EnableJdbcRepositories(
    basePackageClasses = SagaRepository.class,
    transactionManagerRef = DTF_TX_MANAGER,
    jdbcOperationsRef = DTF_JDBC_OPS
)
@EnableTransactionManagement
@EnableAspectJAutoProxy(proxyTargetClass = true)
@EnableConfigurationProperties(value = SagaConfiguration.class)
@ComponentScan(basePackageClasses = ContextMapper.class)
@EnableCaching
public class SagaAutoconfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Clock sagaInternalClock() {
        return Clock.systemUTC();
    }
//
//    //it is important to return exactly SagaContextDiscoveryImpl type in order to allow spring to detect
//    // @Aspect annotation on bean
//    @Bean
//    @ConditionalOnMissingBean
//    public SagaMethodDiscoveryImpl sagaAspect() {
//        return new SagaMethodDiscoveryImpl();
//    }
//
//    @Bean
//    @ConditionalOnMissingBean
//    @Qualifier("commonSagaCaffeineConfig")
//    public Caffeine<Object, Object> commonSagaCaffeineConfig(SagaConfiguration sagaConfiguration) {
//        return Caffeine.newBuilder()
//            .expireAfterWrite(sagaConfiguration.getCommons().getCacheExpiration().toMillis(), TimeUnit.MILLISECONDS);
//    }
//
//    @Bean
//    @ConditionalOnMissingBean
//    @Qualifier("commonSagaCacheManager")
//    public CacheManager commonSagaCacheManager(@Qualifier("commonSagaCaffeineConfig") Caffeine<Object, Object> caffeine) {
//        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
//        caffeineCacheManager.setCaffeine(caffeine);
//        return caffeineCacheManager;
//    }
//
//    @Bean
//    @ConditionalOnMissingBean
//    public SagaManager sagaContextService(DistributedTaskService distributedTaskService,
//                                          SagaRepository sagaRepository,
//                                          DlsSagaContextRepository dlsSagaContextRepository,
//                                          SagaHelper sagaHelper,
//                                          ContextMapper contextMapper,
//                                          @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
//                                          Clock clock,
//                                          SagaConfiguration sagaConfiguration) {
//        return new SagaManagerImpl(
//            distributedTaskService,
//            sagaRepository,
//            dlsSagaContextRepository,
//            sagaHelper,
//            contextMapper,
//            transactionManager,
//            clock,
//            sagaConfiguration
//        );
//    }
//
//    //it is important to return exactly SagaRegisterImpl type in order to allow spring to detect
//    //BeanPostProcessor interface and invoke its method
//    @Bean
//    @ConditionalOnMissingBean
//    public SagaResolverImpl sagaRegister(DistributedTaskService distributedTaskService,
//                                         TaskRegistryService taskRegistryService,
//                                         SagaMethodDiscovery sagaMethodDiscovery,
//                                         SagaTaskFactory sagaTaskFactory,
//                                         SagaConfiguration sagaConfiguration,
//                                         DistributedTaskProperties properties,
//                                         DistributedTaskPropertiesMapper distributedTaskPropertiesMapper,
//                                         DistributedTaskPropertiesMerger distributedTaskPropertiesMerger,
//                                         SagaMethodPropertiesMapper sagaMethodPropertiesMapper) {
//        return new SagaResolverImpl(
//            distributedTaskService,
//            taskRegistryService,
//            sagaMethodDiscovery,
//            sagaTaskFactory,
//            sagaConfiguration,
//            properties,
//            distributedTaskPropertiesMapper,
//            distributedTaskPropertiesMerger,
//            sagaMethodPropertiesMapper
//        );
//    }
//
//    @Bean
//    @ConditionalOnMissingBean
//    public SagaHelper sagaHelper(TaskSerializer taskSerializer) {
//        return new SagaHelper(taskSerializer);
//    }
//
//    @Bean
//    @ConditionalOnMissingBean
//    public DistributionSagaService sagaProcessor(@Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
//                                                 SagaResolver sagaResolver,
//                                                 DistributedTaskService distributedTaskService,
//                                                 SagaManager sagaManager,
//                                                 SagaHelper sagaHelper) {
//        return new DistributionSagaServiceImpl(
//            transactionManager,
//            sagaResolver,
//            distributedTaskService,
//            sagaManager,
//            sagaHelper
//        );
//    }
//
//    @Bean
//    @ConditionalOnMissingBean
//    public SagaTaskFactory sagaTaskFactory(@Lazy SagaResolver sagaResolver,
//                                           DistributedTaskService distributedTaskService,
//                                           TaskSerializer taskSerializer,
//                                           SagaManager sagaManager,
//                                           SagaHelper sagaHelper) {
//        return new SagaTaskFactoryImpl(
//            sagaResolver,
//            distributedTaskService,
//            sagaManager,
//            taskSerializer,
//            sagaHelper
//        );
//    }
}
