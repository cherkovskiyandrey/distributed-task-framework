package com.distributed_task_framework.autoconfigure.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Convenient approach to set task concurrency.
 * Has precedence under default settings and overridden by custom settings in the config
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TaskConcurrency {

    /**
     * How many parallel tasks can be in the cluster.
     *
     * @return
     */
    int maxParallelInCluster() default -1;

    /**
     * How many parallel tasks can be on the one node.
     *
     * @return
     */
    int maxParallelInNode() default -1;
}
