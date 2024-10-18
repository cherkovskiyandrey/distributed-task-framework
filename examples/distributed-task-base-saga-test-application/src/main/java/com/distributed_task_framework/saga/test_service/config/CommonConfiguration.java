package com.distributed_task_framework.saga.test_service.config;

import com.distributed_task_framework.saga.test_service.persistence.repository.AuditRepository;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_TX_MANAGER;

@Configuration
@EnableJdbcRepositories(
    basePackageClasses = AuditRepository.class,
    transactionManagerRef = DTF_TX_MANAGER,
    jdbcOperationsRef = DTF_JDBC_OPS
)
public class CommonConfiguration {
}
