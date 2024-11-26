package com.distributed_task_framework.utils;

import com.distributed_task_framework.local_commands.BatcheableLocalCommand;
import com.distributed_task_framework.local_commands.LocalCommand;
import com.distributed_task_framework.local_commands.TaskBasedLocalCommand;
import com.distributed_task_framework.local_commands.TaskDefBasedLocalCommand;
import com.distributed_task_framework.local_commands.WorkflowBasedLocalCommand;
import com.distributed_task_framework.local_commands.impl.AbstractTaskBasedContextAwareCommand;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import org.instancio.Instancio;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class LocalCommandHelperTest {

    @Test
    void shouldCollapseToBatchedCommandsAndPreserveOrderWhenNotTaskBasedCommands() {
        //when
        var simpleAction1 = new SimpleAction();
        var simpleAction2 = new SimpleAction();
        var simpleAction3 = new SimpleAction();
        var batcheableAction1 = new BatchableAction();
        var batcheableAction2 = new BatchableAction();

        //do
        var batchCommands = LocalCommandHelper.collapseToBatchedCommands(List.of(
                simpleAction1,
                batcheableAction1,
                simpleAction2,
                batcheableAction2,
                simpleAction3
            )
        );

        //then
        assertThat(batchCommands)
            .hasSize(4)
            .containsExactly(
                simpleAction1,
                simpleAction2,
                simpleAction3,
                BatchAction.builder()
                    .batchableAction(batcheableAction1)
                    .batchableAction(batcheableAction2)
                    .build()
            );
    }

    @Test
    void shouldCollapseToBatchTaskBasedCommandsAndPreserveOrderWhenIndependentOnly() {
        //when
        var independentBatcheableTaskBasedAction1 = new BatchableTaskBasedAction(Instancio.create(TaskEntity.class));
        var independentBatcheableTaskBasedAction2 = new BatchableTaskBasedAction(Instancio.create(TaskEntity.class));
        var independentBatcheableTaskBasedAction3 = new BatchableTaskBasedAction(Instancio.create(TaskEntity.class));

        var taskEntity = Instancio.create(TaskEntity.class);
        var dependentBatcheableTaskBasedAction1 = new BatchableTaskBasedAction(taskEntity);
        var dependentBatcheableTaskBasedAction2 = new BatchableTaskBasedAction(taskEntity);
        var dependentBatcheableTaskBasedAction3 = new BatchableTaskBasedAction(taskEntity);

        //do
        var batchCommands = LocalCommandHelper.collapseToBatchedCommands(List.of(
                independentBatcheableTaskBasedAction1,
                dependentBatcheableTaskBasedAction1,
                independentBatcheableTaskBasedAction2,
                dependentBatcheableTaskBasedAction2,
                independentBatcheableTaskBasedAction3,
                dependentBatcheableTaskBasedAction3
            )
        );

        //then
        assertThat(batchCommands)
            .hasSize(4)
            .containsExactly(
                dependentBatcheableTaskBasedAction1,
                dependentBatcheableTaskBasedAction2,
                dependentBatcheableTaskBasedAction3,
                BatchTaskBasedAction.builder()
                    .batchableAction(independentBatcheableTaskBasedAction1)
                    .batchableAction(independentBatcheableTaskBasedAction2)
                    .batchableAction(independentBatcheableTaskBasedAction3)
                    .build()
            );
    }

    @Test
    void shouldNotCollapseToBatchTaskBasedCommandsWhenThereIsTaskDefOne() {
        //when
        var taskEntity = Instancio.create(TaskEntity.class);
        var independentBatcheableTaskBasedAction1 = new BatchableTaskBasedAction(taskEntity);
        var independentBatcheableTaskBasedAction2 = new BatchableTaskBasedAction(Instancio.create(TaskEntity.class));
        var independentBatcheableTaskBasedAction3 = new BatchableTaskBasedAction(Instancio.create(TaskEntity.class));

        var taskDefBasedAction = new TaskDefBasedAction(TaskDef.privateTaskDef(taskEntity.getTaskName()));

        //do
        var batchCommands = LocalCommandHelper.collapseToBatchedCommands(List.of(
                independentBatcheableTaskBasedAction1,
                independentBatcheableTaskBasedAction2,
                independentBatcheableTaskBasedAction3,
                taskDefBasedAction
            )
        );

        //then
        assertThat(batchCommands)
            .hasSize(3)
            .containsExactly(
                independentBatcheableTaskBasedAction1,
                taskDefBasedAction,
                BatchTaskBasedAction.builder()
                    .batchableAction(independentBatcheableTaskBasedAction2)
                    .batchableAction(independentBatcheableTaskBasedAction3)
                    .build()
            );
    }

    @Test
    void shouldCollapseAndPreserverOrderingWhenContextAwareCommand() {
        //when
        var taskEntity1 = Instancio.create(TaskEntity.class);
        var dependentContextAware11 = new BatcheableTaskBasedContextAwareAction(taskEntity1);
        var dependentContextAware12 = new BatcheableTaskBasedContextAwareAction(taskEntity1);
        var dependentContextAware13 = new BatcheableTaskBasedContextAwareAction(taskEntity1);
        var dependentContextAware14 = new BatcheableTaskBasedContextAwareAction(taskEntity1);
        var dependentContextAware15 = new BatcheableTaskBasedContextAwareAction(taskEntity1);

        var taskEntity2 = Instancio.create(TaskEntity.class);
        var dependentContextAware21 = new BatcheableTaskBasedContextAwareAction(taskEntity2);
        var dependentContextAware22 = new BatcheableTaskBasedContextAwareAction(taskEntity2);
        var dependentContextAware23 = new BatcheableTaskBasedContextAwareAction(taskEntity2);
        var dependentContextAware24 = new BatcheableTaskBasedContextAwareAction(taskEntity2);
        var dependentContextAware25 = new BatcheableTaskBasedContextAwareAction(taskEntity2);

        var independentBatcheableTaskBasedAction1 = new BatchableTaskBasedAction(Instancio.create(TaskEntity.class));
        var independentBatcheableTaskBasedAction2 = new BatchableTaskBasedAction(Instancio.create(TaskEntity.class));

        //do
        var batchCommands = LocalCommandHelper.collapseToBatchedCommands(List.of(
                independentBatcheableTaskBasedAction1,
                dependentContextAware11,
                dependentContextAware21,
                dependentContextAware12,
                dependentContextAware22,
                dependentContextAware13,
                dependentContextAware23,
                dependentContextAware14,
                dependentContextAware24,
                dependentContextAware15,
                dependentContextAware25,
                independentBatcheableTaskBasedAction2
            )
        );

        //then
        assertThat(batchCommands)
            .hasSize(3)
            .containsExactly(
                dependentContextAware11,
                dependentContextAware21,
                BatchTaskBasedAction.builder()
                    .batchableAction(independentBatcheableTaskBasedAction1)
                    .batchableAction(independentBatcheableTaskBasedAction2)
                    .build()
            );
        assertThat(((AbstractTaskBasedContextAwareCommand) batchCommands.get(0))
            .getNextCommand()
        )
            .isNotNull()
            .isEqualTo(dependentContextAware12);
        assertThat(((AbstractTaskBasedContextAwareCommand) batchCommands.get(0))
            .getNextCommand()
            .getNextCommand()
        )
            .isNotNull()
            .isEqualTo(dependentContextAware13);
        assertThat(((AbstractTaskBasedContextAwareCommand) batchCommands.get(0))
            .getNextCommand()
            .getNextCommand()
            .getNextCommand()
        )
            .isNotNull()
            .isEqualTo(dependentContextAware14);
        assertThat(((AbstractTaskBasedContextAwareCommand) batchCommands.get(0))
            .getNextCommand()
            .getNextCommand()
            .getNextCommand()
            .getNextCommand()
        )
            .isNotNull()
            .isEqualTo(dependentContextAware15);
    }

    @Test
    void shouldNotCollapseToBatchTaskBasedCommandsWhenThereIsWorkflowBaseOne() {
        //when
        var taskEntity = Instancio.create(TaskEntity.class);
        var independentBatcheableTaskBasedAction1 = new BatchableTaskBasedAction(Instancio.create(TaskEntity.class));
        var dependentBatcheableTaskBasedAction2 = new BatchableTaskBasedAction(taskEntity);
        var independentBatcheableTaskBasedAction3 = new BatchableTaskBasedAction(Instancio.create(TaskEntity.class));

        var workflowBasedAction = new WorkflowBasedAction(taskEntity.getWorkflowId());

        //do
        var batchCommands = LocalCommandHelper.collapseToBatchedCommands(List.of(
                independentBatcheableTaskBasedAction1,
                dependentBatcheableTaskBasedAction2,
                independentBatcheableTaskBasedAction3,
                workflowBasedAction
            )
        );

        //then
        assertThat(batchCommands)
            .hasSize(3)
            .containsExactly(
                dependentBatcheableTaskBasedAction2,
                workflowBasedAction,
                BatchTaskBasedAction.builder()
                    .batchableAction(independentBatcheableTaskBasedAction1)
                    .batchableAction(independentBatcheableTaskBasedAction3)
                    .build()
            );
    }

    @EqualsAndHashCode
    private static class BaseAction implements LocalCommand {

        @Override
        public void execute(InternalTaskCommandService internalTaskCommandService) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasTask(TaskId taskId) {
            throw new UnsupportedOperationException();
        }
    }

    private static class SimpleAction extends BaseAction {
    }

    @Value
    @EqualsAndHashCode(callSuper = true)
    @Builder
    private static class BatchAction extends BaseAction {
        @Singular
        Collection<BatchableAction> batchableActions;
    }

    private static class BatchableAction extends BaseAction implements BatcheableLocalCommand<BatchAction> {

        @Override
        public Class<BatchAction> batchClass() {
            return BatchAction.class;
        }

        @Override
        public BatchAction addToBatch(@Nullable LocalCommandHelperTest.BatchAction batch) {
            if (batch == null) {
                return BatchAction.builder().batchableAction(this).build();
            }
            return BatchAction.builder()
                .batchableActions(batch.getBatchableActions())
                .batchableAction(this)
                .build();
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @RequiredArgsConstructor
    @FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
    private static class BaseTaskBasedAction extends BaseAction implements TaskBasedLocalCommand {
        TaskEntity taskEntity;

        @Override
        public TaskEntity taskEntity() {
            return taskEntity;
        }
    }

    @Value
    @EqualsAndHashCode(callSuper = true)
    @Builder
    private static class BatchTaskBasedAction extends BaseAction {
        @Singular
        Collection<BatchableTaskBasedAction> batchableActions;
    }

    private static class BatchableTaskBasedAction
        extends BaseTaskBasedAction
        implements BatcheableLocalCommand<BatchTaskBasedAction> {

        public BatchableTaskBasedAction(TaskEntity taskEntity) {
            super(taskEntity);
        }

        @Override
        public Class<BatchTaskBasedAction> batchClass() {
            return BatchTaskBasedAction.class;
        }

        @Override
        public BatchTaskBasedAction addToBatch(@Nullable LocalCommandHelperTest.BatchTaskBasedAction batch) {
            if (batch == null) {
                return BatchTaskBasedAction.builder().batchableAction(this).build();
            }
            return BatchTaskBasedAction.builder()
                .batchableActions(batch.getBatchableActions())
                .batchableAction(this)
                .build();
        }
    }

    @Value
    @EqualsAndHashCode(callSuper = true)
    @Builder
    private static class BatchTaskBasedContextAwareAction extends BaseAction {
        @Singular
        Collection<BatcheableTaskBasedContextAwareAction> batchableActions;
    }

    private static class BatcheableTaskBasedContextAwareAction
        extends AbstractTaskBasedContextAwareCommand
        implements BatcheableLocalCommand<BatchTaskBasedContextAwareAction> {

        public BatcheableTaskBasedContextAwareAction(TaskEntity taskEntity) {
            super(taskEntity);
        }

        @Override
        public Class<BatchTaskBasedContextAwareAction> batchClass() {
            return BatchTaskBasedContextAwareAction.class;
        }

        @Override
        public BatchTaskBasedContextAwareAction addToBatch(@Nullable LocalCommandHelperTest.BatchTaskBasedContextAwareAction batch) {
            if (batch == null) {
                return BatchTaskBasedContextAwareAction.builder().batchableAction(this).build();
            }
            return BatchTaskBasedContextAwareAction.builder()
                .batchableActions(batch.getBatchableActions())
                .batchableAction(this)
                .build();
        }

        @Override
        protected TaskEntity doExecute(InternalTaskCommandService internalTaskCommandService, TaskEntity basedTaskEntity) {
            throw new UnsupportedOperationException();
        }
    }


    @EqualsAndHashCode(callSuper = true)
    @RequiredArgsConstructor
    @FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
    private static class TaskDefBasedAction extends BaseAction implements TaskDefBasedLocalCommand {
        TaskDef<?> taskDef;

        @Override
        public TaskDef<?> taskDef() {
            return taskDef;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @RequiredArgsConstructor
    @FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
    private static class WorkflowBasedAction extends BaseAction implements WorkflowBasedLocalCommand {
        UUID workflowId;

        @Override
        public Collection<UUID> workflows() {
            return List.of(workflowId);
        }
    }
}