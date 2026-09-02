package com.distributed_task_framework.saga.autoconfigure.annotations;

import com.distributed_task_framework.saga.autoconfigure.SagaConfigurationDiscoveryProcessor;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.utils.DistributedTaskLifecycleSpringInitializer;
import org.springframework.context.SmartLifecycle;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used in order to mark any method in spring bean as method which can
 * be used in saga transaction.
 * Saga transaction is build via {@link DistributionSagaService}.
 * Use this annotation on any level of class hierarchy: on interface level or on class level,
 * can be public, default or private, because
 * DSF can handle it correctly. Also, you can use direct "this" pointer to provide saga method to DSF framework.
 * For example:
 * <pre>
 * {@code
 * class BusinessService {
 *
 *      @SagaMethod(name = "method")
 *      private String forward(String inputData) {
 *      }
 *
 *      public String calculate(String suffix) {
 *          return distributionSagaService.create("test")
 *                     .registerToRun(this::forward, suffix)
 *                     .start()
 *                     .get();
 *      }
 * }
 * }
 * </pre>
 * Example with revert method:
 * <pre>
 * {@code
 * class BusinessService {
 *
 *      @SagaMethod(name = "forward")
 *      private String forward(String inputData) {
 *      }
 *
 *     @SagaRevertMethod(name = "backward")
 *     private void backward(String val,
 *                          @Nullable String output,
 *                          @Nullable SagaExecutionException sagaExecutionException) {
 *     }
 *
 *      public String calculate(String suffix) {
 *          return distributionSagaService.create("test")
 *                     .registerToRun(this::forward, this::backward, suffix)
 *                     .start()
 *                     .get();
 *      }
 * }
 * }
 * </pre>
 * </br>
 * </br>
 * IMPORTANT: use this annotation only for singleton and not lazy beans or warm-up lazy beans before
 * application context is up and before {@link SmartLifecycle#start()} is called
 * with {@link SmartLifecycle#getPhase()} == Integer.MAX_VALUE
 * (see: {@link DistributedTaskLifecycleSpringInitializer} and {@link SagaConfigurationDiscoveryProcessor})!
 * Otherwise, annotation and bean will be ignored by DSF. This behavior is by design:
 * it is dangerous to allow lazy beans, because can lead potentially to case when beans
 * will not be initialised at all and as result this node will not be able to handle corresponded DTF tasks.
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
