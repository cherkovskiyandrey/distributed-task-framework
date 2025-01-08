package com.distributed_task_framework.saga.settings;

import lombok.Builder;
import lombok.Value;

import java.time.Duration;

@Value
@Builder(toBuilder = true)
public class SagaSettings {
    public static final SagaSettings DEFAULT = SagaSettings.builder().build();

    /**
     * Time between completion of saga and removing of its result from db.
     * In other words: time interval during saga result is available.
     */
    @Builder.Default
    Duration availableAfterCompletionTimeout = Duration.ofMinutes(1);

    /**
     * Default timeout for saga.
     * After timeout expired, the whole saga will be canceled.
     */
    @Builder.Default
    Duration expirationTimeout = Duration.ofHours(1);
}
