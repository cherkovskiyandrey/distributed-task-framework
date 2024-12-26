package com.distributed_task_framework.task;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.StateHolder;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TypeDef;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

public class TaskGenerator {

    public static <T> Task<T> emptyDefineTask(TaskDef<T> taskDef) {
        return new SimpleTestTask<>(taskDef, ctx -> {
        });
    }

    public static <T> Task<T> defineTask(TaskDef<T> taskDef, Consumer<ExecutionContext<T>> action) {
        return new SimpleTestTask<>(taskDef, action);
    }

    public static <T> Task<T> defineTask(TaskDef<T> taskDef,
                                         Consumer<ExecutionContext<T>> action,
                                         Function<FailedExecutionContext<T>, Boolean> failureAction) {
        return new SimpleTestTask<>(taskDef, action, failureAction);
    }

    public static <T, U> StatefulTask<T, U> defineTask(TaskDef<T> taskDef,
                                                       TypeDef<U> stateDef,
                                                       BiConsumer<ExecutionContext<T>, StateHolder<U>> action) {
        return new StatefulTestTask<>(taskDef, stateDef, action);
    }

    public static <T, U> StatefulTask<T, U> defineTask(TaskDef<T> taskDef,
                                                       TypeDef<U> stateDef,
                                                       BiConsumer<ExecutionContext<T>, StateHolder<U>> action,
                                                       BiFunction<FailedExecutionContext<T>, StateHolder<U>, Boolean> failureAction) {
        return new StatefulTestTask<>(taskDef, stateDef, action, failureAction);
    }

    public static <T> Function<FailedExecutionContext<T>, Boolean> interruptedRetry() {
        return fctx -> true;
    }

    @FunctionalInterface
    public interface Consumer<T> {
        void accept(T t) throws Exception;
    }

    @FunctionalInterface
    public interface BiConsumer<T, U> {
        void accept(T t, U u) throws Exception;
    }

    @FunctionalInterface
    public interface Function<IN, OUT> {
        OUT apply(IN in) throws Exception;
    }

    @FunctionalInterface
    public interface BiFunction<IN1, IN2, OUT> {
        OUT apply(IN1 in1, IN2 in2) throws Exception;
    }

    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    public static class SimpleTestTask<T> implements Task<T> {
        TaskDef<T> taskDef;
        Consumer<ExecutionContext<T>> action;
        Function<FailedExecutionContext<T>, Boolean> failureAction;

        public SimpleTestTask(TaskDef<T> taskDef, Consumer<ExecutionContext<T>> action) {
            this.taskDef = taskDef;
            this.action = action;
            this.failureAction = fctx -> false;
        }

        public SimpleTestTask(TaskDef<T> taskDef,
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

    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    public static class StatefulTestTask<T, U> implements StatefulTask<T, U> {
        TaskDef<T> taskDef;
        TypeDef<U> stateDef;
        BiConsumer<ExecutionContext<T>, StateHolder<U>> action;
        BiFunction<FailedExecutionContext<T>, StateHolder<U>, Boolean> failureAction;

        public StatefulTestTask(TaskDef<T> taskDef,
                                TypeDef<U> stateDef,
                                BiConsumer<ExecutionContext<T>, StateHolder<U>> action) {
            this.taskDef = taskDef;
            this.stateDef = stateDef;
            this.action = action;
            this.failureAction = (ctx, hld) -> false;
        }

        public StatefulTestTask(TaskDef<T> taskDef,
                                TypeDef<U> stateDef,
                                BiConsumer<ExecutionContext<T>, StateHolder<U>> action,
                                BiFunction<FailedExecutionContext<T>, StateHolder<U>, Boolean> failureAction) {
            this.taskDef = taskDef;
            this.stateDef = stateDef;
            this.action = action;
            this.failureAction = failureAction;
        }

        @Override
        public TaskDef<T> getDef() {
            return taskDef;
        }

        @Override
        public TypeDef<U> stateDef() {
            return stateDef;
        }

        @Override
        public void execute(ExecutionContext<T> executionContext, StateHolder<U> stateHolder) throws Exception {
            action.accept(executionContext, stateHolder);
        }

        @Override
        public boolean onFailureWithResult(FailedExecutionContext<T> failedExecutionContext, StateHolder<U> stateHolder) throws Exception {
            return failureAction.apply(failedExecutionContext, stateHolder);
        }
    }
}
