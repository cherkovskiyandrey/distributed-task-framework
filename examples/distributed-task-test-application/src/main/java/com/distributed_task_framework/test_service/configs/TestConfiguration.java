package com.distributed_task_framework.test_service.configs;

import com.distributed_task_framework.test_service.persistence.repository.AuditRepository;
import com.distributed_task_framework.test_service.services.impl.SagaContextDiscoveryImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement
@EnableJdbcRepositories(basePackageClasses = AuditRepository.class)
@EnableAspectJAutoProxy //todo: move to autoconfiguration class or replace to manually creating of bean
public class TestConfiguration {

    @Bean
    ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    SagaContextDiscoveryImpl sagaAspect(ApplicationContext applicationContext) {
        return new SagaContextDiscoveryImpl();
    }
}
