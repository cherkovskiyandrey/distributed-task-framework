package com.distributed_task_framework.saga.services.internal;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.google.common.annotations.VisibleForTesting;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Collection;

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
     * @param operation
     * @param <T>
     * @return
     */
    <T extends Serializable> SagaOperand resolveAsOperand(T operation);

    /**
     * Resolve operation to corresponding Method
     *
     * @param methodRef
     * @return
     * @param <T>
     */
    <T extends Serializable> Method findMethodInObject(T methodRef, Object anchorObject);

    /**
     * Resolve registered task definition by task name.
     *
     * @param taskName
     * @return
     */
    TaskDef<SagaPipeline> resolveByTaskName(String taskName);
}
