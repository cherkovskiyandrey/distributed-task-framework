package com.distributed_task_framework.test_service.annotations;

import com.distributed_task_framework.test_service.services.SagaProcessor;

/**
 * Used in order to mark any method in spring bean as method which can
 * be used in saga transaction as revert step.
 * Saga transaction is build via {@link SagaProcessor}.
 * <br>
 * IMPORTANT: take into account that spring generate proxy around such methods.
 * As a result you have to pay attention how to invoke it from the same bean.
 */
public @interface SagaRevertMethod {

    /**
     * The name of saga revert method, agnostic to real java method in class.
     * Used in order to correctly route saga revert action in cluster
     * where current saga logic is locked under different java method name and/or
     * in other class.
     * Has to be unique in cluster. Direct maps to underlined dtf task.
     *
     * @return
     */
    String name();

    /**
     * Version of method.
     * Use in order to distinguish different version of current method
     * which potentially can exist simultaneously in cluster,
     * for example during rolling out of new version of service.
     *
     * @return
     */
    int version() default 0;
}
