package com.distributed_task_framework.task;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.StateHolder;
import com.distributed_task_framework.model.TypeDef;

/**
 * Extended task interface to support internal state.
 * Internal state is used in order to save custom information
 * between execution attempts in scope of one task instance.
 *
 * @param <T>
 * @param <U>
 */
public interface StatefulTask<T, U> extends Task<T> {

    /**
     *
     * @return state definition
     */
    TypeDef<U> stateDef();


    /**
     * The same as {@link Task#execute(ExecutionContext)} but with local state.
     *
     * @param executionContext
     * @param stateHolder
     * @throws Exception
     */
    default void execute(ExecutionContext<T> executionContext, StateHolder<U> stateHolder) throws Exception {
        execute(executionContext);
    }

    /**
     * The same as {@link Task#onFailureWithResult(FailedExecutionContext)} but with local state.
     *
     * @param failedExecutionContext
     * @return
     * @throws Exception
     */
    default boolean onFailureWithResult(FailedExecutionContext<T> failedExecutionContext, StateHolder<U> stateHolder) throws Exception {
        return onFailureWithResult(failedExecutionContext);
    }
}
