package com.distributed_task_framework.autoconfigure;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.repository.config.AbstractJdbcConfiguration;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Optional;

import static org.mockito.Mockito.when;

@SuppressWarnings("ALL")
@Slf4j
@TestConfiguration
@ActiveProfiles("test")
public class SecondaryDataSourceConfiguration extends AbstractJdbcConfiguration {

    @Bean
    @Qualifier
    public DataSource secondaryDataSource() throws SQLException {
        final DataSource dataSource = DataSourceConfigurationUtility.mockDataSource("secondary");
        when(dataSource.toString()).thenReturn("secondary");
        return dataSource;
    }

    @Bean
    @Qualifier
    public PlatformTransactionManager secondaryTransactionManager(@Qualifier DataSource secondaryDataSource) {
        return new DataSourceTransactionManager(secondaryDataSource);
    }

    @Bean
    @Qualifier
    public NamedParameterJdbcOperations secondaryNamedParameterJdbcOperations(@Qualifier DataSource secondaryDataSource) {
        return new NamedParameterJdbcTemplate(secondaryDataSource);
    }

    @Bean("secondaryJdbcDialect")
    @Qualifier
    @Override
    public Dialect jdbcDialect(@Qualifier NamedParameterJdbcOperations secondaryNamedParameterJdbcOperations) {
        return super.jdbcDialect(secondaryNamedParameterJdbcOperations);
    }

    @Bean("secondaryJdbcMappingContext")
    @Qualifier
    @Override
    public JdbcMappingContext jdbcMappingContext(Optional<NamingStrategy> namingStrategy,
                                                 JdbcCustomConversions customConversions,
                                                 RelationalManagedTypes jdbcManagedTypes) {
        return super.jdbcMappingContext(namingStrategy, customConversions, jdbcManagedTypes);
    }

    @Bean("secondaryJdbcConverter")
    @Qualifier
    @Override
    public JdbcConverter jdbcConverter(@Qualifier JdbcMappingContext secondaryJdbcMappingContext,
                                       @Qualifier NamedParameterJdbcOperations secondaryNamedParameterJdbcOperations,
                                       @Lazy RelationResolver relationResolver,
                                       JdbcCustomConversions conversions,
                                       @Qualifier Dialect secondaryJdbcDialect) {
        return super.jdbcConverter(
            secondaryJdbcMappingContext,
            secondaryNamedParameterJdbcOperations,
            relationResolver,
            conversions,
            secondaryJdbcDialect
        );
    }

    @Bean("secondaryDataAccessStrategyBean")
    @Qualifier
    @Override
    public DataAccessStrategy dataAccessStrategyBean(@Qualifier NamedParameterJdbcOperations secondaryNamedParameterJdbcOperations,
                                                     @Qualifier JdbcConverter secondaryJdbcConverter,
                                                     @Qualifier JdbcMappingContext secondaryJdbcMappingContext,
                                                     @Qualifier Dialect secondaryJdbcDialect) {
        return super.dataAccessStrategyBean(
            secondaryNamedParameterJdbcOperations,
            secondaryJdbcConverter,
            secondaryJdbcMappingContext,
            secondaryJdbcDialect
        );
    }
}
