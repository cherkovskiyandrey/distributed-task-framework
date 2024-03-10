package com.distributed_task_framework.settings;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.task.Task;
import lombok.Builder;
import lombok.Value;
import org.springframework.util.StringUtils;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * Configuration parameters for task.
 */
@Value
@Builder(toBuilder = true)
public class TaskSettings {

    public static final TaskSettings DEFAULT = TaskSettings.builder().build();

    /**
     * Execution guarantees.
     */
    @Builder.Default
    ExecutionGuarantees executionGuarantees = ExecutionGuarantees.AT_LEAST_ONCE;

    /**
     * Has this task to be moved to dlt when eventually failed.
     */
    @Builder.Default
    boolean dltEnabled = true;

    /**
     * Cron string.
     */
    @Nullable
    String cron;

    /**
     * Retry policy for task.
     */
    @Builder.Default
    Retry retry = CommonSettings.DEFAULT_RETRY.toBuilder().build();

    /**
     * How many parallel tasks can be in the cluster.
     * '-1' means undefined and depends on current cluster configuration
     * like how many pods work simultaneously.
     */
    @Nullable
    @Builder.Default
    Integer maxParallelInCluster = CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS;

    /**
     * Task timeout. If task still is in progress after timeout expired, it will be interrupted.
     * {@link InterruptedException} will be risen in {@link Task#execute(ExecutionContext)}
     */
    @Nullable
    @Builder.Default
    Duration timeout = Duration.ZERO;

    public boolean hasCron() {
        return StringUtils.hasText(cron);
    }

    public enum ExecutionGuarantees {
        /**
         * Framework guarantee task has been executed at least once.
         * Logic has to be idempotent.
         * Don't open transaction to DB.
         */
        AT_LEAST_ONCE,
        /**
         * Framework guarantee task has been executed only once.
         * Execute task in transaction.
         * Works well for short task mostly deal with DB.
         * Suitable for example to atomically change user balance.
         */
        EXACTLY_ONCE
    }

}
