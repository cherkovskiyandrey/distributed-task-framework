package com.distributed_task_framework.utils;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.internal.BatcheableLocalCommand;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.LocalCommand;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.Value;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

class CommandHelperTest {

    @Test
    void shouldCollapseToBatchedCommands() {
        //when
        var simpleAction1 = new SimpleAction();
        var simpleAction2 = new SimpleAction();
        var batchableAction1 = new BatchableAction();
        var batchableAction2 = new BatchableAction();

        //do
        var batchCommands = CommandHelper.collapseToBatchedCommands(List.of(
                simpleAction1,
                simpleAction2,
                batchableAction1,
                batchableAction2
            )
        );

        //then
        Assertions.assertThat(batchCommands)
            .hasSize(3)
            .containsExactlyInAnyOrder(
                simpleAction1,
                simpleAction2,
                BatchAction.builder()
                    .batchableAction(batchableAction1)
                    .batchableAction(batchableAction2)
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
        public BatchAction addToBatch(@Nullable CommandHelperTest.BatchAction batch) {
            if (batch == null) {
                return BatchAction.builder().batchableAction(this).build();
            }
            return BatchAction.builder()
                .batchableActions(batch.getBatchableActions())
                .batchableAction(this)
                .build();
        }
    }
}