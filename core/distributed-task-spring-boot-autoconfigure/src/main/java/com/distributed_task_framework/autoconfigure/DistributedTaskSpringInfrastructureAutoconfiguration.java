package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Slf4j
@AutoConfiguration
@ConditionalOnClass(DistributedTaskService.class)
@EnableConfigurationProperties(DistributedTaskProperties.class)
@ConditionalOnProperty(name = "distributed-task.enabled", havingValue = "true")
@AutoConfigureAfter(
    value = {
        JdbcTemplateAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
    }
)
@EnableTransactionManagement
public class DistributedTaskSpringInfrastructureAutoconfiguration {

    /**
     * The idea of DtfJdbcInfrastructure to give an ability to application to
     * override default DataSource, PlatformTransactionManager, NamedParameterJdbcOperations,
     * Dialect, DataAccessStrategy only for dtf if there are more than one of DataSources in application.
     *
     * @param platformTransactionManager
     * @param namedParameterJdbcOperations
     * @param dialect
     * @param dataAccessStrategy
     * @param jdbcConverter
     * @param jdbcMappingContext
     * @return
     */
    @Bean
    @ConditionalOnMissingBean(DtfJdbcInfrastructure.class)
    public DtfJdbcInfrastructure dtfJdbcInfrastructure(
        PlatformTransactionManager platformTransactionManager,
        NamedParameterJdbcOperations namedParameterJdbcOperations,
        Dialect dialect,
        DataAccessStrategy dataAccessStrategy,
        JdbcConverter jdbcConverter,
        JdbcMappingContext jdbcMappingContext
    ) {
        return new DtfJdbcInfrastructure(
            platformTransactionManager,
            namedParameterJdbcOperations,
            dialect,
            dataAccessStrategy,
            jdbcMappingContext,
            jdbcConverter
        );
    }
}
