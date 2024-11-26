package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.comparator.RoundingLocalDateTimeComparator;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.exception.UnknownTaskException;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.task.TestTaskModel;
import com.distributed_task_framework.task.TestTaskModelCustomizerUtils;
import com.distributed_task_framework.task.TestTaskModelSpec;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("DataFlowIssue")
@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class TaskCommandRepositoryTest extends BaseRepositoryTest {
    @Autowired
    @Qualifier("taskCommandRepositoryImpl")
    TaskCommandRepository repository;

    @Test
    void shouldReschedule() {
        //when
        var originalTaskEntity = createSimpleTestTaskModelIn(VirtualQueue.READY);
        var toReschedule = originalTaskEntity.getTaskEntity().toBuilder()
            .executionDateUtc(LocalDateTime.now(clock))
            .failures(2)
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
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.READY);
        var toReschedule = testTaskModel.getTaskEntity().toBuilder()
            .version(2L)
            .executionDateUtc(LocalDateTime.now(clock))
            .assignedWorker(UUID.randomUUID())
            .build();

        //do
        assertThatThrownBy(() -> repository.reschedule(toReschedule)).isInstanceOf(OptimisticLockException.class);

        //verify
        verifyInRepository(testTaskModel.getTaskEntity());
    }

    @Test
    void shouldNotRescheduleWhenInDeleted() {
        //when
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.DELETED);
        var toReschedule = testTaskModel.getTaskEntity().toBuilder()
            .version(2L)
            .executionDateUtc(LocalDateTime.now(clock))
            .assignedWorker(UUID.randomUUID())
            .build();

        //do
        assertThatThrownBy(() -> repository.reschedule(toReschedule)).isInstanceOf(UnknownTaskException.class);

        //verify
        verifyInRepository(testTaskModel.getTaskEntity());
    }

    @Test
    void shouldForceReschedule() {
        //when
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.READY);
        var toReschedule = testTaskModel.getTaskEntity().toBuilder()
            .version(10L)
            .failures(2)
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
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.DELETED);
        var toReschedule = testTaskModel.getTaskEntity().toBuilder()
            .version(10L)
            .executionDateUtc(LocalDateTime.now(clock))
            .assignedWorker(UUID.randomUUID())
            .build();

        //do
        boolean result = repository.forceReschedule(toReschedule);

        //verify
        assertThat(result).isFalse();
        verifyInRepository(testTaskModel.getTaskEntity());
    }

    @Test
    void shouldForceRescheduleAll() {
        //when
        var originalTaskEntity = createSimpleTestTaskModelIn(VirtualQueue.READY);
        var toReschedule = originalTaskEntity.getTaskEntity().toBuilder()
            .version(10L)
            .executionDateUtc(LocalDateTime.now(clock))
            .assignedWorker(UUID.randomUUID())
            .build();

        //do
        repository.forceRescheduleAll(List.of(toReschedule));

        //verify
        verifyInRepository(toReschedule);
    }

    @Test
    void shouldForceRescheduleAllWhenInDeleted() {
        //when
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.DELETED);
        var toReschedule = testTaskModel.getTaskEntity().toBuilder()
            .version(10L)
            .executionDateUtc(LocalDateTime.now(clock))
            .assignedWorker(UUID.randomUUID())
            .build();

        //do
        repository.forceRescheduleAll(List.of(toReschedule));

        //verify
        verifyInRepository(testTaskModel.getTaskEntity());
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldForceRescheduleAllByTaskDef() {
        //when
        setFixedTime();
        var timeShift = Duration.ofHours(1);
        var reschedulingTaskModelGroup = generateIndependentTasksInTheSameTaskDef(10);
        var untouchedTaskModel = extendedTaskGenerator.generateDefaultAndSave(String.class);

        //do
        var totalRescheduled = repository.forceRescheduleAll(
            reschedulingTaskModelGroup.get(0).getTaskDef(),
            timeShift,
            List.of(reschedulingTaskModelGroup.get(0).getTaskId())
        );

        //verify
        assertThat(totalRescheduled).isEqualTo(9);
        reschedulingTaskModelGroup.stream().skip(1)
            .forEach(model -> verifyTaskExecutionDate(model.getTaskId(), timeShift));
        verifyTaskExecutionDate(reschedulingTaskModelGroup.get(0).getTaskId(), Duration.ZERO);
        verifyTaskExecutionDate(untouchedTaskModel.getTaskId(), Duration.ZERO);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldNotForceRescheduleAllByTaskDefWhenDeleted() {
        //when
        setFixedTime();
        var timeShift = Duration.ofHours(1);
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.DELETED);

        //do
        var totalRescheduled = repository.forceRescheduleAll(
            testTaskModel.getTaskDef(),
            timeShift,
            List.of()
        );

        //verify
        assertThat(totalRescheduled).isEqualTo(0);
        verifyTaskExecutionDate(testTaskModel.getTaskId(), Duration.ZERO);
    }

    @Test
    void shouldCancel() {
        //when
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.READY);

        //do
        boolean result = repository.cancel(testTaskModel.getTaskId().getId());

        //verify
        assertThat(result).isTrue();
        Assertions.assertThat(taskRepository.find(testTaskModel.getTaskId().getId()))
            .isPresent()
            .get()
            .matches(TaskEntity::isCanceled)
            .matches(taskEntity -> taskEntity.getVersion() > testTaskModel.getTaskEntity().getVersion());
    }

    @Test
    void shouldNotCancelWhenInDeleted() {
        //when
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.DELETED);

        //do
        boolean result = repository.cancel(testTaskModel.getTaskId().getId());

        //verify
        assertThat(result).isFalse();
        verifyInRepository(testTaskModel.getTaskEntity());
    }

    @Test
    void shouldCancelAll() {
        //when
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.READY);

        //do
        repository.cancelAll(List.of(testTaskModel.getTaskId().getId()));

        //verify
        Assertions.assertThat(taskRepository.find(testTaskModel.getTaskId().getId()))
            .isPresent()
            .get()
            .matches(TaskEntity::isCanceled)
            .matches(taskEntity -> taskEntity.getVersion() > testTaskModel.getTaskEntity().getVersion());
    }

    @Test
    void shouldNotCancelAllWhenInDeleted() {
        //when
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.DELETED);

        //do
        repository.cancelAll(List.of(testTaskModel.getTaskId().getId()));

        //verify
        verifyInRepository(testTaskModel.getTaskEntity());
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldCancelAllByTaskDef() {
        //when
        var cancelingTaskModelGroupOne = generateIndependentTasksInTheSameTaskDef(10);
        var untouchedTaskModel = extendedTaskGenerator.generateDefaultAndSave(String.class);

        //do
        var totalCanceled = repository.cancelAll(
            cancelingTaskModelGroupOne.get(0).getTaskDef(),
            List.of(cancelingTaskModelGroupOne.get(0).getTaskId())
        );

        //verify
        assertThat(totalCanceled).isEqualTo(9);
        cancelingTaskModelGroupOne.stream().skip(1).forEach(model -> verifyTaskIsCanceled(model.getTaskId()));
        verifyInRepository(cancelingTaskModelGroupOne.get(0).getTaskEntity());
        verifyInRepository(untouchedTaskModel.getTaskEntity());
    }

    @Test
    void shouldNotCancelAllByTaskDef() {
        //when
        var testTaskModel = createSimpleTestTaskModelIn(VirtualQueue.DELETED);

        //do
        var totalCanceled = repository.cancelAll(
            testTaskModel.getTaskDef(),
            List.of()
        );

        //verify
        assertThat(totalCanceled).isEqualTo(0);
        verifyInRepository(testTaskModel.getTaskEntity());
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldCancelAllByWorkflows() {
        //when
        var cancelingTaskModelGroupOne = generateIndependentTasksInTheSameWorkflow(10);
        var cancelingTaskModelGroupTwo = generateIndependentTasksInTheSameWorkflow(10);
        var untouchedTaskModel = extendedTaskGenerator.generateDefaultAndSave(String.class);

        //do
        var totalCanceled = repository.cancelAll(
            List.of(
                cancelingTaskModelGroupOne.get(0).getTaskId().getWorkflowId(),
                cancelingTaskModelGroupTwo.get(0).getTaskId().getWorkflowId()
            ),
            List.of(cancelingTaskModelGroupOne.get(0).getTaskId())
        );

        //verify
        assertThat(totalCanceled.size()).isEqualTo(19);
        cancelingTaskModelGroupOne.stream().skip(1).forEach(model -> verifyTaskIsCanceled(model.getTaskId()));
        cancelingTaskModelGroupTwo.forEach(model -> verifyTaskIsCanceled(model.getTaskId()));
        verifyInRepository(cancelingTaskModelGroupOne.get(0).getTaskEntity());
        verifyInRepository(untouchedTaskModel.getTaskEntity());
    }

    @Test
    void shouldNotCancelAllByWorkflows() {
        //when
        var originalTestTaskModelOne = createSimpleTestTaskModelIn(VirtualQueue.DELETED);
        var originalTestTaskModelTwo = createSimpleTestTaskModelIn(VirtualQueue.DELETED);

        //do
        var totalCanceled = repository.cancelAll(
            List.of(
                originalTestTaskModelOne.getTaskId().getWorkflowId(),
                originalTestTaskModelTwo.getTaskId().getWorkflowId()
            ),
            List.of()
        );

        //verify
        assertThat(totalCanceled.size()).isEqualTo(0);
        verifyInRepository(originalTestTaskModelOne.getTaskEntity());
        verifyInRepository(originalTestTaskModelTwo.getTaskEntity());
    }

    private TestTaskModel<String> createSimpleTestTaskModelIn(VirtualQueue virtualQueue) {
        return extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .taskEntityCustomizer(TestTaskModelCustomizerUtils.inVirtualQueue(virtualQueue))
            .build()
        );
    }

    private void verifyInRepository(TaskEntity taskEntity) {
        Assertions.assertThat(taskRepository.find(taskEntity.getId()))
            .isPresent()
            .get()
            .usingRecursiveComparison(RecursiveComparisonConfiguration.builder()
                .withComparatorForType(new RoundingLocalDateTimeComparator(), LocalDateTime.class)
                .withIgnoredFields("version")
                .build()
            )
            .isEqualTo(taskEntity);
    }
}