package com.distributed_task_framework.autoconfigure.annotation;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.task.Task;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TaskTimeout {

    /**
     * Task timeout. If task still is in progress after timeout expired, it will be interrupted.
     * {@link InterruptedException} will be risen in {@link Task#execute(ExecutionContext)}
     * Must be compatible with {@link java.time.Duration#parse(CharSequence)}
     *
     * @return
     */
    String value() default "";
}
