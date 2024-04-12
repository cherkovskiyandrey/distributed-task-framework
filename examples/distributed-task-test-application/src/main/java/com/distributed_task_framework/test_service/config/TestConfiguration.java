package com.distributed_task_framework.test_service.config;

import com.distributed_task_framework.autoconfigure.DistributedTaskAutoconfigure;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
//@Import({DistributedTaskAutoconfigure.class})
public class TestConfiguration {

    @Bean
    ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
