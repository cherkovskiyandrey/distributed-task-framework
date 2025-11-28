package com.distributed_task_framework.task;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.utils.TaskGenerator;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

import java.util.UUID;
import java.util.function.Function;


@Getter
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TestTaskModelSpec<T> extends AbstractTaskModelSpec<T> {
    @Nullable
    TaskGenerator.Consumer<ExecutionContext<T>> action;
    @Nullable
    TaskGenerator.Function<FailedExecutionContext<T>, Boolean> failureAction;

    public TestTaskModelSpec(Class<T> inputType,
                             @Nullable TaskDef<T> taskDef,
                             @Nullable Function<TaskSettings, TaskSettings> taskSettingsCustomizer,
                             @Nullable Function<TaskEntity, TaskEntity> taskEntityCustomizer,
                             @Nullable UUID workflowId,
                             boolean saveInstance,
                             boolean recurrent,
                             @Nullable TaskGenerator.Consumer<ExecutionContext<T>> action,
                             @Nullable TaskGenerator.Function<FailedExecutionContext<T>, Boolean> failureAction) {
        super(inputType, taskDef, taskSettingsCustomizer, taskEntityCustomizer, workflowId, saveInstance, recurrent);
        this.action = action;
        this.failureAction = failureAction;
    }

    public static <T> Builder<T> builder(Class<T> inputType) {
        return new Builder<>(inputType);
    }

    public static <T> Builder<T> builder(TaskDef<T> taskDef) {
        return new Builder<>(taskDef);
    }

    @FieldDefaults(level = AccessLevel.PRIVATE)
    public static class Builder<T> extends AbstractBuilder<T, Builder<T>> {
        TaskGenerator.Consumer<ExecutionContext<T>> action;
        TaskGenerator.Function<FailedExecutionContext<T>, Boolean> failureAction;

        protected Builder(Class<T> inputType) {
            super(inputType);
        }

        protected Builder(TaskDef<T> taskDef) {
            super(taskDef);
        }

        @Override
        protected Builder<T> root() {
            return this;
        }

        public Builder<T> action(TaskGenerator.Consumer<ExecutionContext<T>> action) {
            this.action = action;
            return this;
        }

        public Builder<T> failureAction(TaskGenerator.Function<FailedExecutionContext<T>, Boolean> failureAction) {
            this.failureAction = failureAction;
            return this;
        }

        public TestTaskModelSpec<T> build() {
            return new TestTaskModelSpec<>(
                inputType,
                taskDef,
                taskSettingsCustomizer,
                taskEntityCustomizer,
                workflowId,
                saveInstance,
                recurrent,
                action,
                failureAction
            );
        }
    }
}
