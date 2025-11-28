package com.distributed_task_framework.saga.services.internal;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.google.common.annotations.VisibleForTesting;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Optional;

public interface SagaResolver {

    /**
     * Register saga operand.
     *
     * @param name        the name of operand
     * @param sagaOperand operand
     */
    void registerOperand(String name, SagaOperand sagaOperand);

    /**
     * Unregister saga operand if exists.
     *
     * @param name the name of operand
     */
    void unregisterOperand(String name);

    /**
     * Return all names for registered operands.
     *
     * @return
     */
    @VisibleForTesting
    Collection<String> getAllRegisteredOperandNames();

    /**
     * Resolve operation to corresponding SagaOperand if saga element has been registered.
     *
     * @param operation any lambda from package with class {@link com.distributed_task_framework.saga.functions.SagaFunction}
     * @param <T>
     * @return
     */
    <T extends Serializable> SagaOperand resolveAsOperand(T operation);

    /**
     * Resolve operation to corresponding Method
     *
     * @param methodRef
     * @param <T>
     * @return
     */
    <T extends Serializable> Method findMethodInObject(T methodRef, Object anchorObject);

    /**
     * Resolve registered task definition by task name.
     *
     * @param taskName
     * @return
     */
    TaskDef<SagaPipeline> resolveByTaskName(String taskName);

    /**
     * The same as {@link #resolveByTaskName(String)} but not throw exception.
     *
     * @param taskName
     * @return
     */
    Optional<TaskDef<SagaPipeline>> resolveByTaskNameIfExists(String taskName);

    /**
     * Resolve registered task definition by task name in cluster.
     * IMPORTANT: method used in case when task can be absent on current node,
     * but potentially can be registered on other node in cluster.
     *
     * @param taskName
     * @return
     */
    TaskDef<SagaPipeline> resolveByTaskNameInCluster(String taskName);
}
