package com.distributed_task_framework.utils;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DistributedTaskLifecycleSpringConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public static DistributedTaskLifecycleSpringInitializer distributedTaskLifecycleSpringInitializer() {
        return new DistributedTaskLifecycleSpringInitializer();
    }
}
