package com.distributed_task_framework.settings;

import lombok.Builder;
import lombok.Value;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Fixed interval retry policy.
 */
@Value
@Builder(toBuilder = true)
public class Fixed {
    /**
     * Delay between retires.
     */
    @Builder.Default
    Duration delay = Duration.ofSeconds(10);
    /**
     * Max attempts
     */
    @Builder.Default
    Integer maxNumber = 6;
    /**
     * Max interval for retires.
     * Give up after whether max attempts is reached or interval is passed.
     */
    Duration maxInterval;

    public Optional<LocalDateTime> nextRetry(int currentFails, Clock clock) {
        if (currentFails >= maxNumber) {
            return Optional.empty();
        }
        Duration passedTime = Duration.ofSeconds(currentFails * delay.getSeconds());
        if (maxInterval != null && passedTime.compareTo(maxInterval) >= 0) {
            return Optional.empty();
        }
        return Optional.of(LocalDateTime.now(clock).plus(delay));
    }
}
