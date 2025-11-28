package com.distributed_task_framework.task;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.StateHolder;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TypeDef;
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
public class TestStatefulTaskModelSpec<T, U> extends AbstractTaskModelSpec<T> {
    Class<U> stateType;
    TypeDef<U> stateDef;
    @Nullable
    U localState;
    @Nullable
    TaskGenerator.BiConsumer<ExecutionContext<T>, StateHolder<U>> action;
    @Nullable
    TaskGenerator.BiFunction<FailedExecutionContext<T>, StateHolder<U>, Boolean> failureAction;

    public TestStatefulTaskModelSpec(Class<T> inputType,
                                     @Nullable TaskDef<T> taskDef,
                                     @Nullable Function<TaskSettings, TaskSettings> taskSettingsCustomizer,
                                     @Nullable Function<TaskEntity, TaskEntity> taskEntityCustomizer,
                                     @Nullable UUID workflowId,
                                     boolean saveInstance,
                                     boolean recurrent,
                                     Class<U> stateType,
                                     TypeDef<U> stateDef,
                                     U localState,
                                     TaskGenerator.BiConsumer<ExecutionContext<T>, StateHolder<U>> action,
                                     TaskGenerator.BiFunction<FailedExecutionContext<T>, StateHolder<U>, Boolean> failureAction) {
        super(inputType, taskDef, taskSettingsCustomizer, taskEntityCustomizer, workflowId, saveInstance, recurrent);
        this.stateType = stateType;
        this.stateDef = stateDef;
        this.localState = localState;
        this.action = action;
        this.failureAction = failureAction;
    }

    public static <T, U> Builder<T, U> builder(Class<T> inputType, Class<U> stateType) {
        return new Builder<>(inputType, stateType);
    }

    public static <T, U> Builder<T, U> builder(TaskDef<T> taskDef, TypeDef<U> stateDef) {
        return new Builder<>(taskDef, stateDef);
    }

    public static class Builder<T, U> extends AbstractBuilder<T, Builder<T, U>> {
        private final Class<U> stateType;
        TypeDef<U> stateDef;
        U localState;
        TaskGenerator.BiConsumer<ExecutionContext<T>, StateHolder<U>> action;
        TaskGenerator.BiFunction<FailedExecutionContext<T>, StateHolder<U>, Boolean> failureAction;

        protected Builder(Class<T> inputType, Class<U> stateType) {
            super(inputType);
            this.stateType = stateType;
        }

        @SuppressWarnings("unchecked")
        protected Builder(TaskDef<T> taskDef, TypeDef<U> stateDef) {
            super(taskDef);
            this.stateType = (Class<U>) stateDef.getJavaType().getRawClass();
            this.stateDef = stateDef;
        }

        @Override
        protected Builder<T, U> root() {
            return this;
        }

        public Builder<T, U> action(TaskGenerator.BiConsumer<ExecutionContext<T>, StateHolder<U>> action) {
            this.action = action;
            return this;
        }

        public Builder<T, U> failureAction(TaskGenerator.BiFunction<FailedExecutionContext<T>, StateHolder<U>, Boolean> failureAction) {
            this.failureAction = failureAction;
            return this;
        }

        public Builder<T, U> withLocalState(U localState) {
            this.localState = localState;
            return this;
        }

        public TestStatefulTaskModelSpec<T, U> build() {
            return new TestStatefulTaskModelSpec<>(
                inputType,
                taskDef,
                taskSettingsCustomizer,
                taskEntityCustomizer,
                workflowId,
                saveInstance,
                recurrent,
                stateType,
                stateDef,
                localState,
                action,
                failureAction
            );
        }
    }
}
