package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.persistence.entity.IdVersionEntity;
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
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class TaskExtendedRepositoryTest extends BaseRepositoryTest {

    private TaskEntity createTaskEntity() {
        return TaskEntity.builder()
            .taskName("test")
            .workflowId(UUID.randomUUID())
            .affinity("affinity")
            .affinityGroup("affinityGroup")
            .createdDateUtc(LocalDateTime.now(clock))
            .assignedWorker(UUID.randomUUID())
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .lastAssignedDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .notToPlan(true)
            .canceled(true)
            .virtualQueue(VirtualQueue.READY)
            .deletedAt(LocalDateTime.now(clock))
            .messageBytes(new byte[]{1, 2, 3})
            .joinMessageBytes(new byte[]{3, 2, 1})
            .failures(10)
            .build();
    }

    @Test
    void shouldSaveOrUpdate() {
        //when
        var taskEntity = createTaskEntity();

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
        Assertions.assertThat(savedTaskEntity)
            .isPresent()
            .get()
            .usingRecursiveComparison(compareConfiguration)
            .isEqualTo(taskEntity);
    }

    @Test
    void shouldDeleteByIdVersion() {
        //when
        var taskEntity1 = taskRepository.saveOrUpdate(createTaskEntity());
        var taskEntity2 = taskRepository.saveOrUpdate(createTaskEntity());
        var taskEntity3 = taskRepository.saveOrUpdate(createTaskEntity());

        //do
        taskExtendedRepository.deleteByIdVersion(List.of(
            IdVersionEntity.builder().id(taskEntity1.getId()).version(taskEntity1.getVersion()).build(),
            IdVersionEntity.builder().id(taskEntity2.getId()).version(taskEntity2.getVersion() + 1).build(),
            IdVersionEntity.builder().id(UUID.randomUUID()).version(taskEntity1.getVersion()).build()
        ));

        //verify
        assertThat(taskExtendedRepository.find(taskEntity1.getId())).isEmpty();
        assertThat(taskExtendedRepository.find(taskEntity2.getId())).isNotEmpty();
        assertThat(taskExtendedRepository.find(taskEntity3.getId())).isNotEmpty();
    }
}