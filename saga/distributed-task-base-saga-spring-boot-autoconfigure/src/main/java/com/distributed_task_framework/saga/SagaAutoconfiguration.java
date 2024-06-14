package com.distributed_task_framework.saga;


import com.distributed_task_framework.autoconfigure.DistributedTaskAutoconfigure;
import com.distributed_task_framework.saga.configurations.SagaConfiguration;
import com.distributed_task_framework.saga.persistence.repository.SagaResultRepository;
import com.distributed_task_framework.saga.services.SagaContextDiscovery;
import com.distributed_task_framework.saga.services.SagaProcessor;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.services.SagaResultService;
import com.distributed_task_framework.saga.services.SagaTaskFactory;
import com.distributed_task_framework.saga.services.impl.SagaContextDiscoveryImpl;
import com.distributed_task_framework.saga.services.impl.SagaHelper;
import com.distributed_task_framework.saga.services.impl.SagaProcessorImpl;
import com.distributed_task_framework.saga.services.impl.SagaRegisterImpl;
import com.distributed_task_framework.saga.services.impl.SagaResultServiceImpl;
import com.distributed_task_framework.saga.services.impl.SagaTaskFactoryImpl;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.Clock;

@Configuration
@ConditionalOnClass(SagaProcessor.class)
@ConditionalOnProperty(name = "distributed-task.enabled", havingValue = "true")
@AutoConfigureAfter(
        DistributedTaskAutoconfigure.class
)
@EnableJdbcRepositories(basePackageClasses = SagaResultRepository.class)
@EnableTransactionManagement
@EnableAspectJAutoProxy(proxyTargetClass = true)
@EnableConfigurationProperties(value = SagaConfiguration.class)
public class SagaAutoconfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Clock distributedTaskInternalClock() {
        return Clock.systemUTC();
    }

    //it is important to return exactly SagaContextDiscoveryImpl type in order to allow spring to detect
    // @Aspect annotation on bean
    @Bean
    @ConditionalOnMissingBean
    public SagaContextDiscoveryImpl sagaAspect() {
        return new SagaContextDiscoveryImpl();
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaResultService sagaResultService(SagaResultRepository sagaResultRepository,
                                               SagaHelper sagaHelper,
                                               Clock clock,
                                               SagaConfiguration sagaConfiguration) {
        return new SagaResultServiceImpl(
                sagaResultRepository,
                sagaHelper,
                clock,
                sagaConfiguration
        );
    }

    //it is important to return exactly SagaRegisterImpl type in order to allow spring to detect
    //BeanPostProcessor interface and invoke its method
    @Bean
    @ConditionalOnMissingBean
    public SagaRegisterImpl sagaRegister(DistributedTaskService distributedTaskService,
                                         TaskRegistryService taskRegistryService,
                                         SagaContextDiscovery sagaContextDiscovery,
                                         SagaTaskFactory sagaTaskFactory) {
        return new SagaRegisterImpl(
                distributedTaskService,
                taskRegistryService,
                sagaContextDiscovery,
                sagaTaskFactory
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaHelper sagaHelper(TaskSerializer taskSerializer) {
        return new SagaHelper(taskSerializer);
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaProcessor sagaProcessor(PlatformTransactionManager transactionManager,
                                       SagaRegister sagaRegister,
                                       DistributedTaskService distributedTaskService,
                                       SagaResultService sagaResultService,
                                       SagaHelper sagaHelper) {
        return new SagaProcessorImpl(
                transactionManager,
                sagaRegister,
                distributedTaskService,
                sagaResultService,
                sagaHelper
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaTaskFactory sagaTaskFactory(@Lazy SagaRegister sagaRegister,
                                           DistributedTaskService distributedTaskService,
                                           TaskSerializer taskSerializer,
                                           SagaResultService sagaResultService,
                                           SagaHelper sagaHelper) {
        return new SagaTaskFactoryImpl(
                sagaRegister,
                distributedTaskService,
                sagaResultService,
                taskSerializer,
                sagaHelper
        );
    }
}
