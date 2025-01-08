package com.distributed_task_framework.task;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

@AllArgsConstructor
@Getter
@FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
public abstract class AbstractTaskModelSpec<T> {
    public static final Function<TaskEntity, TaskEntity> JOIN_TASK = taskEntity -> taskEntity.toBuilder().notToPlan(true).build();

    Class<T> inputType;
    @Nullable
    TaskDef<T> taskDef;
    @Nullable
    Function<TaskSettings, TaskSettings> taskSettingsCustomizer;
    @Nullable
    Function<TaskEntity, TaskEntity> taskEntityCustomizer;
    @Nullable
    UUID workflowId;
    boolean saveInstance;
    boolean recurrent;

    @FieldDefaults(level = AccessLevel.PROTECTED)
    public static abstract class AbstractBuilder<T, R extends AbstractBuilder<T, R>> {
        protected final Class<T> inputType;
        TaskDef<T> taskDef;
        Function<TaskSettings, TaskSettings> taskSettingsCustomizer;
        Function<TaskEntity, TaskEntity> taskEntityCustomizer;
        UUID workflowId;
        boolean saveInstance;
        boolean recurrent;

        protected AbstractBuilder(Class<T> inputType) {
            this.inputType = inputType;
        }

        @SuppressWarnings("unchecked")
        protected AbstractBuilder(TaskDef<T> taskDef) {
            this.inputType = (Class<T>) taskDef.getInputMessageType().getRawClass();
            this.taskDef = taskDef;
        }

        protected abstract R root();

        public R privateTask(String taskName) {
            this.taskDef = TaskDef.privateTaskDef(taskName, inputType);
            return root();
        }

        public R taskSetting(Function<TaskSettings, TaskSettings> taskSettingsCustomizer) {
            this.taskSettingsCustomizer = taskSettingsCustomizer;
            return root();
        }

        public R recurrent() {
            this.recurrent = true;
            return root();
        }

        public R taskEntityCustomizer(Function<TaskEntity, TaskEntity> taskEntityCustomizer) {
            this.taskEntityCustomizer = taskEntityCustomizer;
            return root();
        }

        public R withSaveInstance() {
            this.saveInstance = true;
            return root();
        }

        public R withSameWorkflowAs(TaskId taskId) {
            Objects.requireNonNull(taskId);
            this.workflowId = taskId.getWorkflowId();
            return root();
        }
    }
}
