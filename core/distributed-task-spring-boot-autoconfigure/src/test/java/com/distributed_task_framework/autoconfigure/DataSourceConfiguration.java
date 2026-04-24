package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.annotation.DtfDataSource;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.mockito.Mockito.when;

@Slf4j
@TestConfiguration
@ActiveProfiles("test")
public class DataSourceConfiguration {
    @Primary
    @Bean
    public DataSource primaryDataSource() throws SQLException {
        final DataSource dataSource = mockDataSource("primary");
        when(dataSource.toString()).thenReturn("primary");
        return dataSource;
    }

    @Bean
    @DtfDataSource
    public DataSource dtfDataSource() throws SQLException {
        final DataSource dataSource = mockDataSource("dtf");
        when(dataSource.toString()).thenReturn("dtf");
        return dataSource;
    }

    @Primary
    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Primary
    @Bean
    public NamedParameterJdbcOperations namedParameterJdbcOperations(DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    private DataSource mockDataSource(String dsName) throws SQLException {
        final DataSource dataSource = Mockito.mock(DataSource.class);
        final Connection connection = Mockito.mock(Connection.class);
        final DatabaseMetaData metadata = Mockito.mock(DatabaseMetaData.class);

        when(dataSource.getConnection()).then(invocation -> {
                log.info(
                    "mockDataSource(): dsName=[{}], stack trace:",
                    dsName,
                    //System.identityHashCode(applicationContext),
                    new Exception("Debug stack trace")
                );
                return connection;
            }
        );
        when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getDatabaseProductName()).thenReturn("PostgreSQL");

        return dataSource;
    }

}
