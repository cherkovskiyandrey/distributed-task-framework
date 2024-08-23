package com.distributed_task_framework.saga.annotations;

import com.distributed_task_framework.saga.services.SagaProcessor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used in order to mark any method in spring bean as method which can
 * be used in saga transaction.
 * Saga transaction is build via {@link SagaProcessor}.
 * <br>
 * IMPORTANT: take into account that spring generate proxy around such methods.
 * As a result you have to pay attention how to invoke it from the same bean.
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SagaMethod {

    /**
     * The name of saga method, agnostic to real java method in class.
     * Used in order to correctly route saga action in cluster
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

    /**
     * List of exceptions saga retry logic not used for.
     * Usually unrecoverable exception where retry doesn't matter.
     *
     * @return
     */
    Class<? extends Throwable>[] noRetryFor() default {};
}
