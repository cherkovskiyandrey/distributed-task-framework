package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.TaskPopulateAndVerify;
import com.distributed_task_framework.comparator.RoundingLocalDateTimeComparator;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.exception.UnknownTaskException;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class TaskCommandRepositoryTest extends BaseRepositoryTest {
    @Autowired
    @Qualifier("taskCommandRepositoryImpl")
    TaskCommandRepository repository;

    @Test
    void shouldReschedule() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.READY);
        var toReschedule = originalTaskEntity.toBuilder()
                .executionDateUtc(LocalDateTime.now(clock))
                .assignedWorker(UUID.randomUUID())
                .build();

        //do
        repository.reschedule(toReschedule);

        //verify
        verifyInRepository(toReschedule);
    }

    @Test
    void shouldNotRescheduleWhenVersionChanged() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.READY);
        var toReschedule = originalTaskEntity.toBuilder()
                .version(2L)
                .executionDateUtc(LocalDateTime.now(clock))
                .assignedWorker(UUID.randomUUID())
                .build();

        //do
        assertThatThrownBy(() -> repository.reschedule(toReschedule)).isInstanceOf(OptimisticLockException.class);

        //verify
        verifyInRepository(originalTaskEntity);
    }

    @Test
    void shouldNotRescheduleWhenInDeleted() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.DELETED);
        var toReschedule = originalTaskEntity.toBuilder()
                .version(2L)
                .executionDateUtc(LocalDateTime.now(clock))
                .assignedWorker(UUID.randomUUID())
                .build();

        //do
        assertThatThrownBy(() -> repository.reschedule(toReschedule)).isInstanceOf(UnknownTaskException.class);

        //verify
        verifyInRepository(originalTaskEntity);
    }

    @Test
    void shouldForceReschedule() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.READY);
        var toReschedule = originalTaskEntity.toBuilder()
                .version(10L)
                .executionDateUtc(LocalDateTime.now(clock))
                .assignedWorker(UUID.randomUUID())
                .build();

        //do
        boolean result = repository.forceReschedule(toReschedule);

        //verify
        assertThat(result).isTrue();
        verifyInRepository(toReschedule);
    }

    @Test
    void shouldForceRescheduleWhenInDeleted() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.DELETED);
        var toReschedule = originalTaskEntity.toBuilder()
                .version(10L)
                .executionDateUtc(LocalDateTime.now(clock))
                .assignedWorker(UUID.randomUUID())
                .build();

        //do
        boolean result = repository.forceReschedule(toReschedule);

        //verify
        assertThat(result).isFalse();
        verifyInRepository(originalTaskEntity);
    }

    @Test
    void shouldRescheduleAllIgnoreVersion() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.READY);
        var toReschedule = originalTaskEntity.toBuilder()
                .version(10L)
                .executionDateUtc(LocalDateTime.now(clock))
                .assignedWorker(UUID.randomUUID())
                .build();

        //do
        repository.rescheduleAllIgnoreVersion(List.of(toReschedule));

        //verify
        verifyInRepository(toReschedule);
    }

    @Test
    void shouldRescheduleAllIgnoreVersionWhenInDeleted() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.DELETED);
        var toReschedule = originalTaskEntity.toBuilder()
                .version(10L)
                .executionDateUtc(LocalDateTime.now(clock))
                .assignedWorker(UUID.randomUUID())
                .build();

        //do
        repository.rescheduleAllIgnoreVersion(List.of(toReschedule));

        //verify
        verifyInRepository(originalTaskEntity);
    }

    @Test
    void shouldCancel() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.READY);

        //do
        boolean result = repository.cancel(originalTaskEntity.getId());

        //verify
        assertThat(result).isTrue();
        Assertions.assertThat(taskRepository.find(originalTaskEntity.getId()))
                .isPresent()
                .get()
                .matches(TaskEntity::isCanceled)
                .matches(taskEntity -> taskEntity.getVersion() > originalTaskEntity.getVersion());
    }

    @Test
    void shouldNotCancelWhenInDeleted() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.DELETED);

        //do
        boolean result = repository.cancel(originalTaskEntity.getId());

        //verify
        assertThat(result).isFalse();
        verifyInRepository(originalTaskEntity);
    }

    @Test
    void shouldCancelAll() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.READY);

        //do
        repository.cancelAll(List.of(originalTaskEntity.getId()));

        //verify
        Assertions.assertThat(taskRepository.find(originalTaskEntity.getId()))
                .isPresent()
                .get()
                .matches(TaskEntity::isCanceled)
                .matches(taskEntity -> taskEntity.getVersion() > originalTaskEntity.getVersion());
    }

    @Test
    void shouldCancelAllWhenInDeleted() {
        //when
        var originalTaskEntity = createSimpleTaskEntity(VirtualQueue.DELETED);

        //do
        repository.cancelAll(List.of(originalTaskEntity.getId()));

        //verify
        verifyInRepository(originalTaskEntity);
    }


    private TaskEntity createSimpleTaskEntity(VirtualQueue virtualQueue) {
        var spec = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask()
                )
        );
        var populateNewTasks = taskPopulateAndVerify.populate(0, 1, virtualQueue, spec);
        return populateNewTasks.iterator().next();
    }

    private void verifyInRepository(TaskEntity taskEntity) {
        Assertions.assertThat(taskRepository.find(taskEntity.getId()))
                .isPresent()
                .get()
                .usingRecursiveComparison(RecursiveComparisonConfiguration.builder()
                        .withIgnoredFields("version")
                        .build()
                )
                .withComparatorForType(new RoundingLocalDateTimeComparator(), LocalDateTime.class)
                .isEqualTo(taskEntity);
    }
}