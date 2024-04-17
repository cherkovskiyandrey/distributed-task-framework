package com.distributed_task_framework.test_service.configs;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.test_service.annotations.SagaMethod;
import com.distributed_task_framework.test_service.models.SagaPipelineContext;
import com.distributed_task_framework.test_service.services.SagaProcessor;
import com.distributed_task_framework.test_service.services.SagaRegister;
import com.distributed_task_framework.test_service.services.impl.SagaHelper;
import com.distributed_task_framework.test_service.services.impl.SagaProcessorImpl;
import com.distributed_task_framework.test_service.services.impl.SagaRevertTask;
import com.distributed_task_framework.test_service.services.impl.SagaTask;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.transaction.PlatformTransactionManager;

import java.lang.reflect.Method;

//todo: autoconfiguration in starter
@Configuration
@FieldDefaults(level = AccessLevel.PRIVATE)
public class SagaConfiguration {
    @Autowired
    DistributedTaskService distributedTaskService;
    @Autowired
    TaskSerializer taskSerializer;

    //todo: BeanPostProcessor doesn't work if create bean from method!
//    @Bean
//    public SagaRegister sagaRegister(DistributedTaskService distributedTaskService,
//                                     SagaContextDiscovery sagaContextDiscovery,
//                                     TaskSerializer taskSerializer) {
//        return new SagaRegisterImpl(
//                distributedTaskService,
//                sagaContextDiscovery,
//                taskSerializer
//        );
//    }

    @Bean
    public SagaHelper sagaHelper(TaskSerializer taskSerializer) {
        return new SagaHelper(taskSerializer);
    }

    @Bean
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

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public SagaTask sagaTask(TaskDef<SagaPipelineContext> taskDef,
                             Method method,
                             Object bean,
                             SagaMethod sagaMethodAnnotation) {
        return new SagaTask(
                distributedTaskService,
                taskSerializer,
                sagaHelper(taskSerializer),
                taskDef,
                method,
                bean,
                sagaMethodAnnotation
        );
    }

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public SagaRevertTask sagaRevertTask(TaskDef<SagaPipelineContext> taskDef,
                                         Method method,
                                         Object bean) {
        return new SagaRevertTask(
                distributedTaskService,
                sagaHelper(taskSerializer),
                taskDef,
                method,
                bean
        );
    }
}
