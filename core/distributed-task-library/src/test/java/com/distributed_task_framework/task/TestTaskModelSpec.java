package com.distributed_task_framework.task;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;
import jakarta.annotation.Nullable;
import lombok.Value;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

@Value
public class TestTaskModelSpec<T> {
    public static final Function<TaskEntity, TaskEntity> JOIN_TASK = taskEntity -> taskEntity.toBuilder().notToPlan(true).build();

    Class<T> inputType;
    @Nullable
    TaskDef<T> taskDef;
    @Nullable
    Function<TaskSettings, TaskSettings> taskSettingsCustomizer;
    @Nullable
    TaskGenerator.Consumer<ExecutionContext<T>> action;
    @Nullable
    TaskGenerator.Function<FailedExecutionContext<T>, Boolean> failureAction;
    @Nullable
    Function<TaskEntity, TaskEntity> taskEntityCustomizer;
    @Nullable
    UUID workflowId;
    boolean saveInstance;
    boolean recurrent;

    public static <T> TaskGenerator.Consumer<ExecutionContext<T>> throwException() {
        return ctx -> {
            throw new RuntimeException();
        };
    }

    public static <T> Builder<T> builder(Class<T> inputType) {
        return new Builder<>(inputType);
    }

    public static class Builder<T> {
        private final Class<T> inputType;
        TaskDef<T> taskDef;
        Function<TaskSettings, TaskSettings> taskSettingsCustomizer;
        TaskGenerator.Consumer<ExecutionContext<T>> action;
        TaskGenerator.Function<FailedExecutionContext<T>, Boolean> failureAction;
        Function<TaskEntity, TaskEntity> taskEntityCustomizer;
        UUID workflowId;
        boolean saveInstance;
        boolean recurrent;

        private Builder(Class<T> inputType) {
            this.inputType = inputType;
        }

        public Builder<T> privateTask(String taskName) {
            this.taskDef = TaskDef.privateTaskDef(taskName, inputType);
            return this;
        }

        public Builder<T> taskSetting(Function<TaskSettings, TaskSettings> taskSettingsCustomizer) {
            this.taskSettingsCustomizer = taskSettingsCustomizer;
            return this;
        }

        public Builder<T> recurrent() {
            this.recurrent = true;
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

        public Builder<T> taskEntityCustomizer(Function<TaskEntity, TaskEntity> taskEntityCustomizer) {
            this.taskEntityCustomizer = taskEntityCustomizer;
            return this;
        }

        public Builder<T> withSaveInstance() {
            this.saveInstance = true;
            return this;
        }

        public Builder<T> withSameWorkflowAs(TaskId taskId) {
            Objects.requireNonNull(taskId);
            this.workflowId = taskId.getWorkflowId();
            return this;
        }

        public TestTaskModelSpec<T> build() {
            return new TestTaskModelSpec<>(
                inputType,
                taskDef,
                taskSettingsCustomizer,
                action,
                failureAction,
                taskEntityCustomizer,
                workflowId,
                saveInstance,
                recurrent
            );
        }
    }
}
