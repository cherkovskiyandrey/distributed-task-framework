package com.distributed_task_framework.saga.test.autoconfiguration;

import com.distributed_task_framework.Signaller;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
@Configuration
public class AdditionalTestConfiguration {

    @Bean
    public Signaller signaller() {
        return new Signaller();
    }
}
