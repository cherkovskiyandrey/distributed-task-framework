package com.distributed_task_framework.utils;

import lombok.Setter;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class TestClock extends Clock {
    @Setter
    private volatile Clock clock;

    public TestClock() {
        this.clock = Clock.systemUTC();
    }

    @Override
    public ZoneId getZone() {
        return clock.getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
        return clock.instant();
    }
}
