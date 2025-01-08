package com.distributed_task_framework.test.autoconfigure;

import com.distributed_task_framework.test.autoconfigure.tasks.DefaultTask;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
@Configuration
@ComponentScan(basePackageClasses = DefaultTask.class)
public class AdditionalTestConfiguration {

    @Bean
    public Signaller signaller() {
        return new Signaller();
    }
}
