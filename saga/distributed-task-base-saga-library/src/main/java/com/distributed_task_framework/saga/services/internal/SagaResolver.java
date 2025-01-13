package com.distributed_task_framework.saga.services.internal;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;

import java.io.Serializable;
import java.lang.reflect.Method;

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
     * @param anchorObject
     * @return
     * @param <T>
     */
    <T extends Serializable> Method resolveAsMethod(T methodRef, Object anchorObject);

    TaskDef<SagaPipeline> resolveByTaskName(String taskName);
}
