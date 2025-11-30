package com.distributed_task_framework.autoconfigure;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.mockito.Mockito.when;


@TestConfiguration
@ActiveProfiles("test")
public class DefaultDataSourceConfiguration {
    @Primary
    @Bean
    public DataSource primaryDataSource() throws SQLException {
        final DataSource dataSource = mockDataSource();
        when(dataSource.toString()).thenReturn("primary");
        return dataSource;
    }

    @Bean
    @Qualifier("secondaryDataSource")
    public DataSource secondaryDataSource() throws SQLException {
        final DataSource dataSource = mockDataSource();
        when(dataSource.toString()).thenReturn("secondary");
        return dataSource;
    }

    private DataSource mockDataSource() throws SQLException {
        final DataSource dataSource = Mockito.mock(DataSource.class);
        final Connection connection = Mockito.mock(Connection.class);
        final DatabaseMetaData metadata = Mockito.mock(DatabaseMetaData.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getDatabaseProductName()).thenReturn("PostgreSQL");

        return dataSource;
    }

}
