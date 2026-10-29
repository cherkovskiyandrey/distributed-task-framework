package com.distributed_task_framework.autoconfigure;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.mockito.Mockito.when;

@Slf4j
@TestConfiguration
@ActiveProfiles("test")
public class DefaultDataSourceConfiguration {
    @Primary
    @Bean
    public DataSource primaryDataSource() throws SQLException {
        final DataSource dataSource = DataSourceConfigurationUtility.mockDataSource("primary");
        when(dataSource.toString()).thenReturn("primary");
        return dataSource;
    }

    @Bean
    @Qualifier
    public DataSource secondaryDataSource() throws SQLException {
        final DataSource dataSource = DataSourceConfigurationUtility.mockDataSource("secondary");
        when(dataSource.toString()).thenReturn("secondary");
        return dataSource;
    }
}
