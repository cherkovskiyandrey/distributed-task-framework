package com.distributed_task_framework.saga;

import com.distributed_task_framework.TestClock;
import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMapper;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMerger;
import com.distributed_task_framework.saga.configurations.SagaConfiguration;
import com.distributed_task_framework.saga.mappers.ContextMapper;
import com.distributed_task_framework.saga.mappers.SagaMethodPropertiesMapper;
import com.distributed_task_framework.saga.persistence.repository.DlsSagaContextRepository;
import com.distributed_task_framework.saga.persistence.repository.SagaContextRepository;
import com.distributed_task_framework.saga.services.internal.SagaContextDiscovery;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.SagaFactory;
import com.distributed_task_framework.saga.services.internal.SagaRegister;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.saga.services.impl.SagaContextDiscoveryImpl;
import com.distributed_task_framework.saga.services.impl.SagaHelper;
import com.distributed_task_framework.saga.services.impl.SagaManagerImpl;
import com.distributed_task_framework.saga.services.impl.SagaFactoryImpl;
import com.distributed_task_framework.saga.services.impl.SagaRegisterImpl;
import com.distributed_task_framework.saga.services.impl.SagaTaskFactoryImpl;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.jdbc.repository.config.EnableJdbcAuditing;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_TX_MANAGER;


@Configuration
@EnableJdbcAuditing
@EnableJdbcRepositories(
    basePackageClasses = SagaContextRepository.class,
    transactionManagerRef = DTF_TX_MANAGER,
    jdbcOperationsRef = DTF_JDBC_OPS
)
@EnableTransactionManagement
@EnableAspectJAutoProxy(proxyTargetClass = true)
@EnableConfigurationProperties(value = SagaConfiguration.class)
@ComponentScan(basePackageClasses = ContextMapper.class)
@EnableCaching
public class BaseTestConfiguration {

    @Bean
    public TestClock sagaInternalClock() {
        return new TestClock();
    }


    //it is important to return exactly SagaContextDiscoveryImpl type in order to allow spring to detect
    // @Aspect annotation on bean
    @Bean
    public SagaContextDiscoveryImpl sagaAspect() {
        return new SagaContextDiscoveryImpl();
    }

    @Bean
    @Qualifier("commonSagaCaffeineConfig")
    public Caffeine<Object, Object> commonSagaCaffeineConfig() {
        return Caffeine.newBuilder().expireAfterWrite(0, TimeUnit.MILLISECONDS);
    }

    @Bean
    @Qualifier("commonSagaCacheManager")
    public CacheManager commonSagaCacheManager(@Qualifier("commonSagaCaffeineConfig") Caffeine<Object, Object> caffeine) {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeine(caffeine);
        return caffeineCacheManager;
    }

    @Bean
    public SagaManager sagaContextService(DistributedTaskService distributedTaskService,
                                          SagaContextRepository sagaContextRepository,
                                          DlsSagaContextRepository dlsSagaContextRepository,
                                          SagaHelper sagaHelper,
                                          ContextMapper contextMapper,
                                          @Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                          Clock clock,
                                          SagaConfiguration sagaConfiguration) {
        return new SagaManagerImpl(
            distributedTaskService,
            sagaContextRepository,
            dlsSagaContextRepository,
            sagaHelper,
            contextMapper,
            transactionManager,
            clock,
            sagaConfiguration
        );
    }

    //it is important to return exactly SagaRegisterImpl type in order to allow spring to detect
    //BeanPostProcessor interface and invoke its method
    @Bean
    public SagaRegisterImpl sagaRegister(DistributedTaskService distributedTaskService,
                                         TaskRegistryService taskRegistryService,
                                         SagaContextDiscovery sagaContextDiscovery,
                                         SagaTaskFactory sagaTaskFactory,
                                         SagaConfiguration sagaConfiguration,
                                         DistributedTaskProperties properties,
                                         DistributedTaskPropertiesMapper distributedTaskPropertiesMapper,
                                         DistributedTaskPropertiesMerger distributedTaskPropertiesMerger,
                                         SagaMethodPropertiesMapper sagaMethodPropertiesMapper) {
        return new SagaRegisterImpl(
            distributedTaskService,
            taskRegistryService,
            sagaContextDiscovery,
            sagaTaskFactory,
            sagaConfiguration,
            properties,
            distributedTaskPropertiesMapper,
            distributedTaskPropertiesMerger,
            sagaMethodPropertiesMapper
        );
    }

    @Bean
    public SagaHelper sagaHelper(TaskSerializer taskSerializer) {
        return new SagaHelper(taskSerializer);
    }

    @Bean
    public SagaFactory sagaProcessor(@Qualifier(DTF_TX_MANAGER) PlatformTransactionManager transactionManager,
                                     SagaRegister sagaRegister,
                                     DistributedTaskService distributedTaskService,
                                     SagaManager sagaManager,
                                     SagaHelper sagaHelper) {
        return new SagaFactoryImpl(
            transactionManager,
            sagaRegister,
            distributedTaskService,
            sagaManager,
            sagaHelper
        );
    }

    @Bean
    public SagaTaskFactory sagaTaskFactory(@Lazy SagaRegister sagaRegister,
                                           DistributedTaskService distributedTaskService,
                                           TaskSerializer taskSerializer,
                                           SagaManager sagaManager,
                                           SagaHelper sagaHelper) {
        return new SagaTaskFactoryImpl(
            sagaRegister,
            distributedTaskService,
            sagaManager,
            taskSerializer,
            sagaHelper
        );
    }
}
