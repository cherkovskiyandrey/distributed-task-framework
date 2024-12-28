package com.distributed_task_framework.saga.configurations;

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
import java.util.Map;
import java.util.UUID;

@Validated
@Data
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@NoArgsConstructor
@ConfigurationProperties(prefix = "distributed-task.saga")
public class SagaConfiguration {

    @Builder.Default
    Common commons = Common.builder().build();

    @Builder.Default
    Context context = Context.builder().build();

    @Builder.Default
    Map<String, SagaProperties> sagaPropertiesGroup = Map.of();

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
        @Builder.Default
        Duration cacheExpiration = Duration.ofSeconds(1);
    }

    @Validated
    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Context {
        /**
         * Initial delay to start scan deprecation sagas: completed and expired.
         */
        @Builder.Default
        Duration deprecatedSagaScanInitialDelay = Duration.ofSeconds(10);
        /**
         * Fixed delay to scan deprecation sagas: completed and expired.
         */
        @Builder.Default
        Duration deprecatedSagaScanFixedDelay = Duration.ofSeconds(10);
        /**
         * Time between completion of saga and removing of its result from db.
         * In other words: time interval during saga result is available.
         */
        @Builder.Default
        Duration completedTimeout = Duration.ofMinutes(1);
        /**
         * Default timeout for any saga.
         * After timout is expired, the whole saga will be canceled.
         * Can be customized in {@link SagaConfiguration.SagaProperties#expirationTimeout}
         */
        @Builder.Default
        Duration expirationTimeout = Duration.ofHours(1);
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
    }

    @Validated
    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class SagaProperties {
        /**
         * The whole saga expiration timeout.
         */
        Duration expirationTimeout;
    }
}
