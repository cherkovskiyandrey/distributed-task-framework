package com.distributed_task_framework.perf_test;

import com.distributed_task_framework.perf_test.persistence.repository.StressTestSummaryRepository;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootConfiguration
@EnableAutoConfiguration
@EnableJdbcRepositories(
    basePackageClasses = StressTestSummaryRepository.class
)
@EnableTransactionManagement
@ComponentScan(basePackageClasses = PerfTestRootPackage.class)
public class BaseTestConfiguration {
}
