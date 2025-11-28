package com.distributed_task_framework.utils;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

import java.util.List;
import java.util.function.Supplier;

public interface MetricHelper {

    String buildName(String... paths);

    Timer timer(String... pathsName);

    Timer timer(List<String> pathsName, List<Tag> tags);

    Counter counter(String... pathsName);

    Counter counter(List<String> pathsName, List<Tag> tags);

    Gauge gauge(List<String> pathsName, Supplier<Number> supplier);

    Gauge gauge(List<String> pathsName, List<Tag> tags, Supplier<Number> supplier);
}
