package com.distributed_task_framework.saga;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Validated
@Data
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@NoArgsConstructor
@ConfigurationProperties(prefix = "distributed-task.saga")
public class DistributedSagaProperties {
    /**
     * Common properties for library.
     */
    Common common;

    /**
     * Properties for any saga and for particular one.
     */
    SagaPropertiesGroup sagaPropertiesGroup;

    /**
     * Properties for any saga method and for particular one.
     */
    SagaMethodPropertiesGroup sagaMethodPropertiesGroup;

    @Validated
    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Common {

        /**
         * Cache expiration for internal purpose.
         * Mostly to cover cases when user invoke method like {@link SagaManager#isCompleted(UUID)}
         */
        Duration cacheExpiration;

        /**
         * Initial delay to start scan deprecation sagas: completed and expired.
         */
        Duration deprecatedSagaScanInitialDelay;

        /**
         * Fixed delay to scan deprecation sagas: completed and expired.
         */
        Duration deprecatedSagaScanFixedDelay;
    }

    @Validated
    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class SagaMethodPropertiesGroup {
        SagaMethodProperties defaultSagaMethodProperties;
        @Builder.Default
        Map<String, SagaMethodProperties> sagaMethodProperties = Map.of();
    }

    @Validated
    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class SagaMethodProperties {

        /**
         * Execution guarantees.
         */
        TaskSettings.ExecutionGuarantees executionGuarantees;

        /**
         * Retry policy for saga-task.
         */
        DistributedTaskProperties.Retry retry;

        /**
         * How many parallel saga methods can be in the cluster.
         * '-1' means undefined and depends on current cluster configuration
         * like how many pods work simultaneously.
         */
        Integer maxParallelInCluster;

        /**
         * Task timeout. If saga method still is in progress after timeout expired, it will be interrupted.
         * {@link InterruptedException} will be risen in {@link Task#execute(ExecutionContext)}
         */
        Duration timeout;

        /**
         * List of exceptions saga retry logic not used for.
         * Usually unrecoverable exception where retry doesn't matter.
         */
        @Builder.Default
        List<Class<? extends Throwable>> noRetryFor = Lists.newArrayList();
    }

    @Validated
    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class SagaPropertiesGroup {
        /**
         * Default properties for any saga.
         */
        SagaProperties defaultSagaProperties;

        /**
         * Custom properties for particular saga by name.
         * Overrides default one.
         */
        @Builder.Default
        Map<String, SagaProperties> sagaPropertiesGroup = Map.of();
    }

    @Validated
    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class SagaProperties {

        /**
         * Time between completion of saga and removing of its result from db.
         * In other words: time interval during saga result is available.
         */
        Duration availableAfterCompletionTimeout;

        /**
         * Default timeout for saga.
         * After timeout expired, the whole saga will be canceled.
         */
        Duration expirationTimeout;
    }
}
