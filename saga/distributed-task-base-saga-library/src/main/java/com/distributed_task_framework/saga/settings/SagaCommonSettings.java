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
     * Fixed delay to scan deprecation sagas: completed and expired.
     */
    @Builder.Default
    Duration deprecatedSagaScanFixedDelay = Duration.ofSeconds(10);

    /**
     * The size of batch of expired saga entities in db to handle (remove) in one tick.
     */
    @Builder.Default
    Integer expiredSagaBatchSizeToRemove = 1000;
}
