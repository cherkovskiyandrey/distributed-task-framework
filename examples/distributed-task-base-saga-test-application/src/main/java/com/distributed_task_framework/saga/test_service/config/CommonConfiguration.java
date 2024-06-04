package com.distributed_task_framework.saga.test_service.config;

import com.distributed_task_framework.saga.test_service.persistence.repository.AuditRepository;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;

@Configuration
@EnableJdbcRepositories(basePackageClasses = AuditRepository.class)
public class CommonConfiguration {
}
