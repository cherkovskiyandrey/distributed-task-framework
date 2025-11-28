package com.distributed_task_framework.saga.settings;

import lombok.Builder;
import lombok.Value;

import java.time.Duration;

@Value
@Builder(toBuilder = true)
public class SagaStatSettings {
    public static final SagaStatSettings DEFAULT = SagaStatSettings.builder().build();

    public static SagaStatSettings buildDefault() {
        return SagaStatSettings.builder().build();
    }

    /**
     * Delay between calculation of saga statistics.
     */
    @Builder.Default
    Duration calcInitialDelay = Duration.ofSeconds(10);

    /**
     * Delay between calculation of saga statistics.
     */
    @Builder.Default
    Duration calcFixedDelay = Duration.ofSeconds(10);

    /**
     * Top N saga names to print in stat.
     */
    @Builder.Default
    Integer topNSagas = 15;
}
