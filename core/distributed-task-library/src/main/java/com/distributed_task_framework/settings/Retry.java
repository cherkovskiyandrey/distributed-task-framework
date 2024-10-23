package com.distributed_task_framework.settings;

import lombok.Builder;
import lombok.Value;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Optional;

@Value
@Builder(toBuilder = true)
public class Retry {
    RetryMode retryMode;
    Fixed fixed;
    Backoff backoff;

    public Optional<LocalDateTime> nextRetry(int currentFails, Clock clock) {
        if (retryMode == RetryMode.OFF) {
            return Optional.empty();
        } else if (retryMode == RetryMode.BACKOFF) {
            return backoff.nextRetry(currentFails, clock);
        } else if (retryMode == RetryMode.FIXED) {
            return fixed.nextRetry(currentFails, clock);
        }
        return Optional.empty();
    }
}
