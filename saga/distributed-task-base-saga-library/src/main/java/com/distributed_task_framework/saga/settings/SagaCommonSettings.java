package com.distributed_task_framework.saga.settings;

import com.distributed_task_framework.saga.services.internal.SagaManager;
import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.util.UUID;

@Value
@Builder(toBuilder = true)
public class SagaCommonSettings {
    public static SagaCommonSettings buildDefault() {
        return SagaCommonSettings.builder().build();
    }

    /**
     * Cache expiration for internal purpose.
     * Mostly to cover cases when user invoke method like {@link SagaManager#isCompleted(UUID)}
     */
    @Builder.Default
    Duration cacheExpiration = Duration.ofSeconds(1);

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
}
