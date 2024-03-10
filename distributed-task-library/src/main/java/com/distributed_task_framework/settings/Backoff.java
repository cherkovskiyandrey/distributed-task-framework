package com.distributed_task_framework.settings;

import lombok.Builder;
import lombok.Value;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Exponential backoff retry policy.
 * nextRetry = failCount == 1 ? currentTime + initialDelay : currentTime + delayPeriod*2^(failCount-1)
 * where failCount = 1, 2, 3, ... nextRetry = Min(nextRetry, currentTime + maxDelay)
 */
@Value
@Builder(toBuilder = true)
public class Backoff {
    /**
     * Initial delay of the first retry.
     */
    @Builder.Default
    Duration initialDelay = Duration.ofSeconds(10);
    /**
     * The time interval that is the ratio of the exponential backoff formula (geometric progression)
     */
    @Builder.Default
    Duration delayPeriod = Duration.ofSeconds(10);
    /**
     * Maximum number of times a tuple is retried before being acked and scheduled for commit.
     */
    @Builder.Default
    int maxRetries = 32;
    /**
     * Maximum amount of time waiting before retrying.
     */
    @Builder.Default
    Duration maxDelay = Duration.ofHours(1);

    // nextRetry = failCount == 1 ? currentTime + initialDelay : currentTime + delayPeriod*2^(failCount-1)
    public Optional<LocalDateTime> nextRetry(int currentFails, Clock clock) {
        if (currentFails == 0 || currentFails > maxRetries) {
            return Optional.empty();
        }
        long nowMillis = clock.millis();
        long nextRetryMillis = currentFails == 1 ?
                nowMillis + initialDelay.toMillis() :
                nowMillis + (long) (delayPeriod.toMillis() * Math.pow(2, currentFails - 1));

        nextRetryMillis = Math.min(Math.abs(nextRetryMillis), nowMillis + maxDelay.toMillis());
        return Optional.of(Instant.ofEpochMilli(nextRetryMillis).atZone(clock.getZone()).toLocalDateTime());
    }
}
