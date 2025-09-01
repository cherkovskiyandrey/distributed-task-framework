package com.distributed_task_framework.perf_test;

import com.distributed_task_framework.perf_test.persistence.repository.StressTestSummaryRepository;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_JDBC_OPS;
import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_TX_MANAGER;

@SpringBootConfiguration
@EnableAutoConfiguration
@EnableJdbcRepositories(
    basePackageClasses = StressTestSummaryRepository.class,
    transactionManagerRef = DTF_TX_MANAGER,
    jdbcOperationsRef = DTF_JDBC_OPS
)
@EnableTransactionManagement
@EnableCaching
@ComponentScan(basePackageClasses = PerfTestRootPackage.class)
public class BaseTestConfiguration {
}
