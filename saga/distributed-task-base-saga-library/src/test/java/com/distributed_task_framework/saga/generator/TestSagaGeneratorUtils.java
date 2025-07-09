package com.distributed_task_framework.saga.generator;

import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.settings.Fixed;
import com.distributed_task_framework.settings.Retry;
import com.distributed_task_framework.settings.RetryMode;
import com.google.common.collect.Lists;
import lombok.experimental.UtilityClass;

import java.time.Duration;

@UtilityClass
public class TestSagaGeneratorUtils {

    public SagaMethodSettings withoutRetry() {
        return SagaMethodSettings.buildDefault().toBuilder()
            .retry(Retry.builder()
                .retryMode(RetryMode.OFF)
                .build()
            )
            .build();
    }

    public SagaMethodSettings withRetry(int retry) {
        return SagaMethodSettings.buildDefault().toBuilder()
            .retry(Retry.builder()
                .retryMode(RetryMode.FIXED)
                .fixed(Fixed.builder()
                    .delay(Duration.ofMillis(100))
                    .maxNumber(retry)
                    .build()
                )
                .build()
            )
            .build();
    }

    @SafeVarargs
    public SagaMethodSettings withNoRetryFor(Class<? extends Throwable>... exceptions) {
        return SagaMethodSettings.buildDefault().toBuilder()
            .noRetryFor(Lists.newArrayList(exceptions))
            .build();
    }

    public static SagaSettings withAvailableAfterCompletionTimeout(Duration duration) {
        return SagaSettings.DEFAULT.toBuilder()
            .availableAfterCompletionTimeout(duration)
            .build();
    }

    public static SagaSettings withExpirationTimeout(Duration duration) {
        return SagaSettings.DEFAULT.toBuilder()
            .expirationTimeout(duration)
            .build();
    }

    public static SagaSettings withStopOnFailedAnyRevert(boolean stopOnFailedAnyRevert) {
        return SagaSettings.DEFAULT.toBuilder()
            .stopOnFailedAnyRevert(stopOnFailedAnyRevert)
            .build();
    }
}
