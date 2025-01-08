package com.distributed_task_framework.saga.settings;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.Retry;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.util.List;

@Value
@Builder(toBuilder = true)
public class SagaMethodSettings {
    public static final SagaMethodSettings DEFAULT = SagaMethodSettings.builder().build();

    /**
     * Execution guarantees.
     */
    @Builder.Default
    TaskSettings.ExecutionGuarantees executionGuarantees = TaskSettings.ExecutionGuarantees.AT_LEAST_ONCE;

    /**
     * Retry policy for saga-task.
     */
    @Builder.Default
    Retry retry = CommonSettings.DEFAULT_RETRY.toBuilder().build();

    /**
     * How many parallel saga methods can be in the cluster.
     * '-1' means undefined and depends on current cluster configuration
     * like how many pods work simultaneously.
     */
    @Nullable
    @Builder.Default
    Integer maxParallelInCluster = CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS;

    /**
     * Task timeout. If saga method still is in progress after timeout expired, it will be interrupted.
     * {@link InterruptedException} will be risen in {@link Task#execute(ExecutionContext)}
     */
    @Builder.Default
    Duration timeout = Duration.ZERO;

    /**
     * List of exceptions saga retry logic not used for.
     * Usually unrecoverable exception where retry doesn't matter.
     */
    @Builder.Default
    List<Class<? extends Throwable>> noRetryFor = List.of();
}
