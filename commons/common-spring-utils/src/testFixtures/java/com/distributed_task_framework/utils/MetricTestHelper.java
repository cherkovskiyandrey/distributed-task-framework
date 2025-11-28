package com.distributed_task_framework.utils;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.groups.Tuple;

import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class MetricTestHelper {
    private final MeterRegistry meterRegistry;

    public MetricTestHelper(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void assertMetricToContain(String metricName, Function<Meter, ?>[] mappers, Tuple... tuples) {
        List<Meter> gaugesForAll = meterRegistry.getMeters().stream()
            .filter(meter -> metricName.equals(meter.getId().getName()))
            .toList();
        log.info(
            "metrics: {}",
            gaugesForAll.stream()
                .map(meter -> "%s: %s".formatted(meter.getId(), meter.measure().iterator().next().getValue()))
                .toList()
        );
        assertThat(gaugesForAll).hasSize(tuples.length)
            .map(mappers)
            .containsExactlyInAnyOrder(tuples);
    }

    public void assertMetricNotExists(String metricName) {
        List<Meter> gaugesForAll = meterRegistry.getMeters().stream()
            .filter(meter -> metricName.equals(meter.getId().getName()))
            .toList();
        assertThat(gaugesForAll).isEmpty();
    }

    public void assertMetricNotExists(String metricName, Function<Meter, ?>[] mappers, Tuple... tuples) {
        List<Meter> gaugesForAll = meterRegistry.getMeters().stream()
            .filter(meter -> metricName.equals(meter.getId().getName()))
            .toList();
        assertThat(gaugesForAll)
            .map(mappers)
            .doesNotContain(tuples);
    }

    public void assertMetric(String metricName, int number) {
        assertThat((int)meterRegistry.get(metricName).meter().measure().iterator().next().getValue())
            .isEqualTo(number);
    }
}
