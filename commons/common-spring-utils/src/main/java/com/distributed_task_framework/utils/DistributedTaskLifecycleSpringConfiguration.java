package com.distributed_task_framework.utils;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DistributedTaskLifecycleSpringConfiguration {

    @Bean
    public static DistributedTaskLifecycleSpringInitializer distributedTaskLifecycleSpringInitializer() {
        return new DistributedTaskLifecycleSpringInitializer();
    }
}
