package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.annotation.DtfDataSource;
import com.distributed_task_framework.persistence.repository.RemoteCommandRepository;
import com.distributed_task_framework.persistence.repository.jdbc.TaskStatRepositoryImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
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
        DataSourceConfiguration.class,
        DataSourceAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class,
        DistributedTaskAutoconfigure.class
})
@EnableAutoConfiguration
public class DataSourceTest {
    @Autowired
    DataSource primaryDataSource;

    @Autowired
    @DtfDataSource
    DataSource dtfDataSource;

    @Autowired
    TaskStatRepositoryImpl taskStatRepository;

    @Autowired
    RemoteCommandRepository commandRepository;

    @Test
    void shouldUseAnnotatedDataSourceInCustomRepository() throws SQLException {
        clearInvocations(primaryDataSource, dtfDataSource);

        try {
            taskStatRepository.getAggregatedTaskStat(Set.of("task1"));
        } catch (NullPointerException e) {
            // expected NPE from mock
        }

        assertUsedDtf();
    }

    @Test
    void shouldUseAnnotatedDataSourceInCrudRepository() throws SQLException {
        clearInvocations(primaryDataSource, dtfDataSource);

        try {
            commandRepository.findCommandsToSend("test", LocalDateTime.now(), 100);
        } catch (NullPointerException e) {
            // expected NPE from mock
        }

        assertUsedDtf();
    }

    private void assertUsedDtf() throws SQLException {
        assertThat(primaryDataSource).isNotSameAs(dtfDataSource);

        verify(dtfDataSource, atLeastOnce()).getConnection();
        verifyNoInteractions(primaryDataSource);
    }

}
