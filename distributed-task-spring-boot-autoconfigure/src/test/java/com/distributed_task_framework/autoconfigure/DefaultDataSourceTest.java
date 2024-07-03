package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.persistence.repository.RemoteCommandRepository;
import com.distributed_task_framework.persistence.repository.jdbc.TaskStatRepositoryImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ActiveProfiles("test")
@SpringBootTest(
        properties = {
        "distributed-task.enabled=true",
        "distributed-task.common.app-name=test"
})
@ContextConfiguration(classes = {
        DefaultDataSourceConfiguration.class,
        DataSourceAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class,
        DistributedTaskAutoconfigure.class
})
@EnableAutoConfiguration
public class DefaultDataSourceTest {
    @Autowired
    DataSource primaryDataSource;

    @Autowired
    @Qualifier("secondaryDataSource")
    DataSource secondaryDataSource;

    @Autowired
    TaskStatRepositoryImpl customRepository;

    @Autowired
    RemoteCommandRepository crudRepository;

    @Test
    void shouldUseDefaultDataSourceInCustomRepository() throws SQLException {
        clearInvocations(primaryDataSource, secondaryDataSource);

        try {
            customRepository.getAggregatedTaskStat(Set.of("task1"));
        } catch (NullPointerException e) {
            // expected NPE from mock
        }

        assertUsedPrimary();
    }

    @Test
    void shouldUseDefaultDataSourceInCrudRepository() throws SQLException {
        clearInvocations(primaryDataSource, secondaryDataSource);

        try {
            crudRepository.findCommandsToSend("test", LocalDateTime.now(), 100);
        } catch (NullPointerException e) {
            // expected NPE from mock
        }

        assertUsedPrimary();
    }

    private void assertUsedPrimary() throws SQLException {
        assertThat(primaryDataSource).isNotSameAs(secondaryDataSource);

        verify(primaryDataSource, atLeastOnce()).getConnection();
        verifyNoInteractions(secondaryDataSource);
    }

}
