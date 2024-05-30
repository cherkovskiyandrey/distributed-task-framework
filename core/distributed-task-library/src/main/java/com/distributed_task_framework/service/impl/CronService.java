package com.distributed_task_framework.service.impl;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Optional;

public class CronService {
    Clock clock;
    CronParser cronParser;

    public CronService(Clock clock) {
        this.clock = clock;
        this.cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING));
    }

    public Optional<LocalDateTime> nextExecutionDate(String cron, boolean considerCurrentMoment) {
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
