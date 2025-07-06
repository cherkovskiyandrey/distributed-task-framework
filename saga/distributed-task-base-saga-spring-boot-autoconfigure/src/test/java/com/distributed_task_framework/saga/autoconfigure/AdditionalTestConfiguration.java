package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.saga.autoconfigure.services.SagaSpecificTestService;
import com.distributed_task_framework.saga.autoconfigure.services.impl.ExternalSagaTestServiceImpl;
import com.distributed_task_framework.saga.autoconfigure.services.impl.SagaSpecificTestServiceImpl;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
@Configuration
@ComponentScan(basePackageClasses = ExternalSagaTestServiceImpl.class)
public class AdditionalTestConfiguration {

    @Bean
    @Qualifier("sagaSpecificTestServiceOne")
    public SagaSpecificTestService sagaSpecificTestServiceOne(DistributionSagaService distributionSagaService) {
        return new SagaSpecificTestServiceImpl(distributionSagaService, "one");
    }

    @Bean
    @Qualifier("sagaSpecificTestServiceTwo")
    public SagaSpecificTestService sagaSpecificTestServiceTwo(DistributionSagaService distributionSagaService) {
        return new SagaSpecificTestServiceImpl(distributionSagaService, "two");
    }
}
