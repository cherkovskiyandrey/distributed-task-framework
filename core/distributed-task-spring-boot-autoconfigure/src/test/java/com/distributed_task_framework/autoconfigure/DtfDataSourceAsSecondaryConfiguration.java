package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.utils.DtfJdbcInfrastructure;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@TestConfiguration
@AutoConfigureAfter(SecondaryDataSourceConfiguration.class)
@Import(SecondaryDataSourceConfiguration.class)
@ActiveProfiles("test")
public class DtfDataSourceAsSecondaryConfiguration {

    @Bean
    @ConditionalOnMissingBean(DtfJdbcInfrastructure.class)
    public DtfJdbcInfrastructure dtfJdbcInfrastructure(
        @Qualifier PlatformTransactionManager secondaryTransactionManager,
        @Qualifier NamedParameterJdbcOperations secondaryNamedParameterJdbcOperations,
        @Qualifier Dialect secondaryJdbcDialect,
        @Qualifier DataAccessStrategy secondaryDataAccessStrategy,
        @Qualifier JdbcConverter secondaryJdbcConverter,
        @Qualifier JdbcMappingContext jdbcMappingContext
    ) {
        return new DtfJdbcInfrastructure(
            secondaryTransactionManager,
            secondaryNamedParameterJdbcOperations,
            secondaryJdbcDialect,
            secondaryDataAccessStrategy,
            jdbcMappingContext,
            secondaryJdbcConverter
        );
    }
}
