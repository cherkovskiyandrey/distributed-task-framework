package com.distributed_task_framework.utils;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.StateHolder;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TypeDef;
import com.distributed_task_framework.task.StatefulTask;
import com.distributed_task_framework.task.Task;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TaskGenerator {

    public static <T> Task<T> emptyDefineTask(TaskDef<T> taskDef) {
        return new SimpleTask<>(taskDef, ctx -> {
        });
    }

    public static <T> Task<T> defineTask(TaskDef<T> taskDef, Consumer<ExecutionContext<T>> action) {
        return new SimpleTask<>(
            taskDef,
            action,
            fctx -> {
                log.error("onFailure(): error", fctx.getError());
                return false;
            }
        );
    }

    public static <T> Task<T> defineTask(TaskDef<T> taskDef,
                                         Consumer<ExecutionContext<T>> action,
                                         Function<FailedExecutionContext<T>, Boolean> failureAction) {
        return new SimpleTask<>(taskDef, action, failureAction);
    }

    public static <T, U> StatefulTask<T, U> defineTask(TaskDef<T> taskDef,
                                                       TypeDef<U> stateDef,
                                                       BiConsumer<ExecutionContext<T>, StateHolder<U>> action) {
        return new StatefulSimpleTask<>(taskDef, stateDef, action);
    }

    public static <T, U> StatefulTask<T, U> defineTask(TaskDef<T> taskDef,
                                                       TypeDef<U> stateDef,
                                                       BiConsumer<ExecutionContext<T>, StateHolder<U>> action,
                                                       BiFunction<FailedExecutionContext<T>, StateHolder<U>, Boolean> failureAction) {
        return new StatefulSimpleTask<>(taskDef, stateDef, action, failureAction);
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
    public static class SimpleTask<T> implements Task<T> {
        TaskDef<T> taskDef;
        Consumer<ExecutionContext<T>> action;
        Function<FailedExecutionContext<T>, Boolean> failureAction;

        public SimpleTask(TaskDef<T> taskDef, Consumer<ExecutionContext<T>> action) {
            this.taskDef = taskDef;
            this.action = action;
            this.failureAction = fctx -> false;
        }

        public SimpleTask(TaskDef<T> taskDef,
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
    public static class StatefulSimpleTask<T, U> implements StatefulTask<T, U> {
        TaskDef<T> taskDef;
        TypeDef<U> stateDef;
        BiConsumer<ExecutionContext<T>, StateHolder<U>> action;
        BiFunction<FailedExecutionContext<T>, StateHolder<U>, Boolean> failureAction;

        public StatefulSimpleTask(TaskDef<T> taskDef,
                                  TypeDef<U> stateDef,
                                  BiConsumer<ExecutionContext<T>, StateHolder<U>> action) {
            this.taskDef = taskDef;
            this.stateDef = stateDef;
            this.action = action;
            this.failureAction = (ctx, hld) -> false;
        }

        public StatefulSimpleTask(TaskDef<T> taskDef,
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
