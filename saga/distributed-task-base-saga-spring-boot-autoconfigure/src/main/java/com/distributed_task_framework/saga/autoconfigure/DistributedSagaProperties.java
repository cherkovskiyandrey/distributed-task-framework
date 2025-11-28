package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
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
     * Properties to configure saga statistics.
     */
    SagaStatProperties sagaStatProperties;

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
         * Fixed delay to scan deprecation sagas: completed and expired.
         */
        Duration deprecatedSagaScanFixedDelay;

        /**
         * The size of batch of expired saga entities in db to handle (remove) in one tick.
         */
        Integer expiredSagaBatchSizeToRemove;
    }

    @Validated
    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class SagaStatProperties {
        /**
         * Delay between calculation of saga statistics.
         */
        Duration calcInitialDelay;

        /**
         * Delay between calculation of saga statistics.
         */
        Duration calcFixedDelay;

        /**
         * Top N saga names to print in stat.
         */
        Integer topNSagas;
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
         * 1. Default is not set in config at all. In this case field equals null - means not set.
         * And value will be used from low precedence configs.
         * 2. If is set in code as empty list or not empty - means override all low precedence values.
         */
        @Builder.Default
        List<Class<? extends Throwable>> noRetryFor = null;
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

        /**
         * If any revert operation fail, then stop execute reverting chain and complete saga.
         */
        Boolean stopOnFailedAnyRevert;
    }
}
