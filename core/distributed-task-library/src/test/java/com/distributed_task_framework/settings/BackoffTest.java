package com.distributed_task_framework.settings;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffTest {
    Clock clock;

    @BeforeEach
    void init() {
        clock = Clock.fixed(Instant.ofEpochSecond(0), ZoneId.of("UTC"));
    }

    @Test
    void shouldReturnNextRetry() {
        //when
        Backoff backoff = Backoff.builder()
                .initialDelay(Duration.ofSeconds(10))
                .delayPeriod(Duration.ofSeconds(10))
                .maxDelay(Duration.ofHours(1))
                .build();

        //do
        // 0 + 10*2^(5 - 1) = 10*2^4 = 10*16 = 160 sec = 2 mins 40 secs
        Optional<LocalDateTime> localDateTimeOpt = backoff.nextRetry(5, clock);

        //verify
        assertThat(localDateTimeOpt)
                .isPresent()
                .get()
                .satisfies(localDateTime -> assertThat(localDateTime)
                        .isEqualTo(LocalDateTime.now(clock).plusMinutes(2).plusSeconds(40))
                );
    }

    @Test
    void shouldNotExceedMaxDelayWhenMaxRetriesIsHuge() {
        //when
        Backoff backoff = Backoff.builder()
                .initialDelay(Duration.ofSeconds(10))
                .delayPeriod(Duration.ofSeconds(10))
                .maxDelay(Duration.ofHours(1))
                .maxRetries(1_000_000)
                .build();

        //do
        Optional<LocalDateTime> localDateTimeOpt = backoff.nextRetry(444_967, clock);

        //verify
        assertThat(localDateTimeOpt)
                .isPresent()
                .get()
                .satisfies(localDateTime -> assertThat(localDateTime)
                        .isEqualTo(LocalDateTime.now(clock).plusHours(1))
                );
    }

    @Test
    void shouldStopWhenMaxRetriesExceed() {
        //when
        Backoff backoff = Backoff.builder()
                .initialDelay(Duration.ofSeconds(10))
                .delayPeriod(Duration.ofSeconds(10))
                .maxDelay(Duration.ofHours(1))
                .maxRetries(1_000_000)
                .build();

        //do
        Optional<LocalDateTime> localDateTimeOpt = backoff.nextRetry(1_000_001, clock);

        //verify
        assertThat(localDateTimeOpt).isEmpty();
    }

    @Test
    void shouldNotStartReprocessingWhenRetryIsZero() {
        //when
        Backoff backoff = Backoff.builder()
                .initialDelay(Duration.ofSeconds(10))
                .delayPeriod(Duration.ofSeconds(10))
                .maxDelay(Duration.ofHours(1))
                .maxRetries(1_000_000)
                .build();

        //do
        Optional<LocalDateTime> localDateTimeOpt = backoff.nextRetry(0, clock);

        //verify
        assertThat(localDateTimeOpt).isEmpty();
    }
}