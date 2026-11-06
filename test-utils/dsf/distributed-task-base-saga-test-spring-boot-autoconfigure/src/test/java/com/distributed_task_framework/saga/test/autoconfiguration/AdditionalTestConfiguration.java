package com.distributed_task_framework.saga.test.autoconfiguration;

import com.distributed_task_framework.saga.test.autoconfiguration.test_data.service.SagaMethodProviderService;
import com.distributed_task_framework.utils.Signaller;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
@Configuration
@ComponentScan(basePackageClasses = SagaMethodProviderService.class)
public class AdditionalTestConfiguration {

    @Bean
    public Signaller signaller() {
        return new Signaller();
    }
}
