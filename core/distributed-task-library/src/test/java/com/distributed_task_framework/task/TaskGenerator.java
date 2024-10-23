package com.distributed_task_framework.task;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;

public class TaskGenerator {

    public static <T> Task<T> emptyDefineTask(TaskDef<T> taskDef) {
        return new MyTask<>(taskDef, ctx -> {
        });
    }

    public static <T> Task<T> defineTask(TaskDef<T> taskDef, Consumer<ExecutionContext<T>> action) {
        return new MyTask<>(taskDef, action);
    }

    public static <T> Task<T> defineTask(TaskDef<T> taskDef,
                                         Consumer<ExecutionContext<T>> action,
                                         Function<FailedExecutionContext<T>, Boolean> failureAction) {
        return new MyTask<>(taskDef, action, failureAction);
    }

    public static <T> Function<FailedExecutionContext<T>, Boolean> interruptedRetry() {
        return fctx -> true;
    }

    @FunctionalInterface
    public interface Consumer<T> {
        void accept(T t) throws Exception;
    }

    @FunctionalInterface
    public interface Function<IN, OUT> {
        OUT apply(IN in) throws Exception;
    }

    public static class MyTask<T> implements Task<T> {
        private final TaskDef<T> taskDef;
        private final Consumer<ExecutionContext<T>> action;
        private final Function<FailedExecutionContext<T>, Boolean> failureAction;

        public MyTask(TaskDef<T> taskDef, Consumer<ExecutionContext<T>> action) {
            this.taskDef = taskDef;
            this.action = action;
            this.failureAction = fctx -> false;
        }

        public MyTask(TaskDef<T> taskDef,
                      Consumer<ExecutionContext<T>> action,
                      Function<FailedExecutionContext<T>, Boolean> failureAction) {
            this.taskDef = taskDef;
            this.action = action;
            this.failureAction = failureAction;
        }

        @Override
        public TaskDef<T> getDef() {
            return taskDef;
        }

        @Override
        public void execute(ExecutionContext<T> executionContext) throws Exception {
            action.accept(executionContext);
        }

        @Override
        public boolean onFailureWithResult(FailedExecutionContext<T> failedExecutionContext) throws Exception {
            return failureAction.apply(failedExecutionContext);
        }
    }
}
