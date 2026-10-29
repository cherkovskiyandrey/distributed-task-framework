package com.distributed_task_framework.autoconfigure;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mockito;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.mockito.Mockito.when;

@Slf4j
@UtilityClass
public class DataSourceConfigurationUtility {

    public DataSource mockDataSource(String dsName) throws SQLException {
        final DataSource dataSource = Mockito.mock(DataSource.class);
        final Connection connection = Mockito.mock(Connection.class);
        final DatabaseMetaData metadata = Mockito.mock(DatabaseMetaData.class);

        when(dataSource.getConnection()).then(invocation -> {
                log.info(
                    "mockDataSource(): dsName=[{}], stack trace:",
                    dsName,
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
