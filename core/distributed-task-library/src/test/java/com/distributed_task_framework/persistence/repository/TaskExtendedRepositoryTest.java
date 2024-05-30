package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class TaskExtendedRepositoryTest extends BaseRepositoryTest {

    @Test
    void shouldSaveOrUpdate() {
        //when
        var taskEntity = TaskEntity.builder()
                .taskName("test")
                .workflowId(UUID.randomUUID())
                .affinity("affinity")
                .affinityGroup("affinityGroup")
                .createdDateUtc(LocalDateTime.now(clock))
                .assignedWorker(UUID.randomUUID())
                .workflowCreatedDateUtc(LocalDateTime.now(clock))
                .lastAssignedDateUtc(LocalDateTime.now(clock))
                .executionDateUtc(LocalDateTime.now(clock))
                .singleton(true)
                .notToPlan(true)
                .canceled(true)
                .virtualQueue(VirtualQueue.READY)
                .deletedAt(LocalDateTime.now(clock))
                .messageBytes(new byte[]{1, 2, 3})
                .joinMessageBytes(new byte[]{3, 2, 1})
                .failures(10)
                .build();

        //do
        var returnedTaskEntity = taskExtendedRepository.saveOrUpdate(taskEntity);

        //verify
        var compareConfiguration = RecursiveComparisonConfiguration.builder()
                .withIgnoredFields(
                        TaskEntity.Fields.id,
                        TaskEntity.Fields.version
                )
                .withComparatorForType(BaseSpringIntegrationTest.LOCAL_DATE_TIME_COMPARATOR_TO_SECONDS, LocalDateTime.class)
                .withComparatorForType(Arrays::compare, byte[].class)
                .build();
        Assertions.assertThat(returnedTaskEntity)
                .usingRecursiveComparison(compareConfiguration)
                .isEqualTo(taskEntity);

        var savedTaskEntity = taskExtendedRepository.find(returnedTaskEntity.getId());
        assertThat(savedTaskEntity)
                .isPresent()
                .get()
                .usingRecursiveComparison(compareConfiguration)
                .isEqualTo(taskEntity);
    }
}