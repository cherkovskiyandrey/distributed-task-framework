package com.distributed_task_framework.saga;


import com.distributed_task_framework.autoconfigure.DistributedTaskAutoconfigure;
import com.distributed_task_framework.saga.services.SagaContextDiscovery;
import com.distributed_task_framework.saga.services.SagaProcessor;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.services.SagaTaskFactory;
import com.distributed_task_framework.saga.services.impl.SagaContextDiscoveryImpl;
import com.distributed_task_framework.saga.services.impl.SagaHelper;
import com.distributed_task_framework.saga.services.impl.SagaProcessorImpl;
import com.distributed_task_framework.saga.services.impl.SagaRegisterImpl;
import com.distributed_task_framework.saga.services.impl.SagaTaskFactoryImpl;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Lazy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@ConditionalOnClass(SagaProcessor.class)
@ConditionalOnProperty(name = "distributed-task.enabled", havingValue = "true")
//@EnableConfigurationProperties //todo: add own properties
@AutoConfigureAfter(
        DistributedTaskAutoconfigure.class
)
//@EnableJdbcRepositories(basePackageClasses = NodeStateRepository.class) //todo: add link to repo
@EnableTransactionManagement
@EnableAspectJAutoProxy
public class SagaAutoconfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SagaContextDiscovery sagaAspect() {
        return new SagaContextDiscoveryImpl();
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaRegister sagaRegister(DistributedTaskService distributedTaskService,
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
                                       SagaHelper sagaHelper) {
        return new SagaProcessorImpl(
                transactionManager,
                sagaRegister,
                distributedTaskService,
                sagaHelper
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaTaskFactory sagaTaskFactory(@Lazy SagaRegister sagaRegister,
                                           DistributedTaskService distributedTaskService,
                                           TaskSerializer taskSerializer,
                                           SagaHelper sagaHelper) {
        return new SagaTaskFactoryImpl(
                sagaRegister,
                distributedTaskService,
                taskSerializer,
                sagaHelper
        );
    }
}
