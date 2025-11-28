package com.distributed_task_framework.service.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CronServiceTest {
    Clock clock = Clock.fixed(Instant.ofEpochSecond(0), ZoneId.of("UTC"));
    CronService cronService = new CronService(clock);

    @Test
    void shouldReturnTrueWhenIsValidCronOrDurationExpressionAndCronIsValid() {
        assertThat(cronService.isValidCronOrDurationExpression("*/10 * * * * *")).isTrue();
    }

    @Test
    void shouldReturnFalseWhenIsValidCronOrDurationExpressionAndCronIsInvalid() {
        assertThat(cronService.isValidCronOrDurationExpression("*/10 * * * *")).isFalse();
    }

    @Test
    void shouldReturnTrueWhenIsValidCronOrDurationExpressionAndDurationIsValid() {
        assertThat(cronService.isValidCronOrDurationExpression("PT10S")).isTrue();
    }

    @Test
    void shouldReturnFalseWhenIsValidCronOrDurationExpressionAndDurationIsInvalid() {
        assertThat(cronService.isValidCronOrDurationExpression("invalid string")).isFalse();
    }

    private static Stream<Arguments> shouldHandleNextExecutionDateCorrectlyArguments() {
        return Stream.of(
            Arguments.of("*/10 * * * * *", true, Duration.ofSeconds(0)),
            Arguments.of("*/10 * * * * *", false, Duration.ofSeconds(10)),
            Arguments.of("PT10S", true, Duration.ofSeconds(10))
        );
    }

    @ParameterizedTest
    @MethodSource("shouldHandleNextExecutionDateCorrectlyArguments")
    void shouldHandleNextExecutionDateCorrectly(String expression,
                                                boolean considerCurrentMoment,
                                                Duration addToCheck) {
        //when & do
        var opt = cronService.nextExecutionDate(expression, considerCurrentMoment);

        //verify
        assertThat(opt)
            .isPresent()
            .hasValue(LocalDateTime.now(clock).plus(addToCheck));
    }
}