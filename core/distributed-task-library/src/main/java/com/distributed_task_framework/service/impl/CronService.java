package com.distributed_task_framework.service.impl;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.springframework.scheduling.support.CronExpression;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class CronService {
    Clock clock;
    CronParser cronParser;

    public CronService(Clock clock) {
        this.clock = clock;
        this.cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING));
    }

    boolean isValidCronOrDurationExpression(String expression) {
        if (CronExpression.isValidExpression(expression)) {
            return true;
        }
        try {
            Duration.parse(expression);
        } catch (DateTimeParseException exception) {
            return false;
        }
        return true;
    }

    public Optional<LocalDateTime> nextExecutionDate(String cron, boolean considerCurrentMoment) {
        if (CronExpression.isValidExpression(cron)) {
            return nextExecutionDateFromCron(cron, considerCurrentMoment);
        }
        return nextExecutionDateForDuration(cron);
    }

    private Optional<LocalDateTime> nextExecutionDateForDuration(String cron) {
        var duration = Duration.parse(cron);
        return Optional.of(LocalDateTime.now(clock).plus(duration));
    }

    private Optional<LocalDateTime> nextExecutionDateFromCron(String cron, boolean considerCurrentMoment) {
        Cron parsedCron = cronParser.parse(cron);
        ExecutionTime executionTime = ExecutionTime.forCron(parsedCron);
        ZonedDateTime now = ZonedDateTime.now(clock);
        boolean canBeExecutedNow = executionTime.isMatch(now);
        if (considerCurrentMoment && canBeExecutedNow) {
            return Optional.of(LocalDateTime.now(clock));
        }
        return executionTime.nextExecution(now)
            .map(ZonedDateTime::toLocalDateTime);
    }
}
