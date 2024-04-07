package com.distributed_task_framework.test_service.configs;

import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.test_service.services.SagaContextDiscovery;
import com.distributed_task_framework.test_service.services.SagaProcessor;
import com.distributed_task_framework.test_service.services.SagaRegister;
import com.distributed_task_framework.test_service.services.impl.SagaHelper;
import com.distributed_task_framework.test_service.services.impl.SagaProcessorImpl;
import com.distributed_task_framework.test_service.services.impl.SagaRegisterImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

//todo: autoconfiguration in starter
@Configuration
public class SagaConfiguration {

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
}
