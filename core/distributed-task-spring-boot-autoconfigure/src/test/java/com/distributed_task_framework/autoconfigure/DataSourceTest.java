package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.persistence.repository.TestDataRepository;
import com.distributed_task_framework.persistence.repository.NodeStateRepository;
import com.distributed_task_framework.persistence.repository.RemoteCommandRepository;
import com.distributed_task_framework.persistence.repository.jdbc.TaskStatRepositoryImpl;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.UUID;

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
    PrimaryDataSourceConfiguration.class,
    SecondaryDataSourceConfiguration.class,
    DtfDataSourceAsSecondaryConfiguration.class,
    DataSourceAutoConfiguration.class,
    JdbcTemplateAutoConfiguration.class,
    DistributedTaskSpringInfrastructureAutoconfiguration.class,
    DistributedTaskAutoConfiguration.class
})
@EnableJdbcRepositories(basePackageClasses = TestDataRepository.class)
@EnableAutoConfiguration
public class DataSourceTest {
    @Autowired
    DataSource primaryDataSource;

    @Autowired
    @Qualifier
    DataSource secondaryDataSource;

    @Autowired
    TaskStatRepositoryImpl taskStatRepository;

    @Autowired
    RemoteCommandRepository commandRepository;

    @Autowired
    NodeStateRepository nodeStateRepository;

    @Autowired
    TestDataRepository testDataRepository;

    @SneakyThrows
    @Test
    void shouldUsePrimaryDataSourceInNotDtfRepository() {
        clearInvocations(primaryDataSource, secondaryDataSource);

        try {
            testDataRepository.findById(UUID.randomUUID());
        } catch (NullPointerException e) {
            // expected NPE from mock
        }

        assertUsedPrimary();
    }

    @SneakyThrows
    @Test
    void shouldUseSecondaryDataSourceInCustomRepository() {
        clearInvocations(primaryDataSource, secondaryDataSource);

        try {
            taskStatRepository.getAggregatedTaskStat(Set.of("task1"));
        } catch (NullPointerException e) {
            // expected NPE from mock
        }

        assertUsedSecondary();
    }

    @Test
    void shouldUseSecondaryDataSourceInCrudRepository() throws SQLException {
        clearInvocations(primaryDataSource, secondaryDataSource);

        try {
            commandRepository.findCommandsToSend("test", LocalDateTime.now(), 100);
        } catch (NullPointerException e) {
            // expected NPE from mock
        }

        assertUsedSecondary();
    }

    private void assertUsedSecondary() throws SQLException {
        assertThat(primaryDataSource).isNotSameAs(secondaryDataSource);

        verify(secondaryDataSource, atLeastOnce()).getConnection();
        verifyNoInteractions(primaryDataSource);
    }

    private void assertUsedPrimary() throws SQLException {
        assertThat(primaryDataSource).isNotSameAs(secondaryDataSource);

        verify(primaryDataSource, atLeastOnce()).getConnection();
        verifyNoInteractions(secondaryDataSource);
    }
}
