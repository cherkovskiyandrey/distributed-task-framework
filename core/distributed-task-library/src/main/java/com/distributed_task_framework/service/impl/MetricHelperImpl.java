package com.distributed_task_framework.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.StringUtils;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.service.internal.MetricHelper;

import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class MetricHelperImpl implements MetricHelper {
    MeterRegistry meterRegistry;

    @Override
    public String virtualQueueStatName(@Nullable VirtualQueue virtualQueue) {
        return virtualQueue != null ? virtualQueue.toString().toLowerCase() : UNDEFINED_VIRTUAL_QUEUE_NAME;
    }

    @Override
    public Tag buildVirtualQueueTag(@Nullable VirtualQueue virtualQueue) {
        return Tag.of("virtualQueue", virtualQueueStatName(virtualQueue));
    }

    @Override
    public String affinityGroupStatName(@Nullable String value) {
        return StringUtils.defaultIfBlank(value, DEFAULT_GROUP_TAG_NAME);
    }

    @Override
    public Tag buildAffinityGroupTag(String value) {
        return Tag.of("affinity_group", affinityGroupStatName(value));
    }

    @Override
    public String buildName(String... paths) {
        return String.join(".", paths);
    }

    @Override
    public Timer timer(String... pathsName) {
        return timer(Lists.newArrayList(pathsName), List.of());
    }

    @Override
    public Timer timer(List<String> pathsName, List<Tag> tags) {
        return Timer.builder(buildName(pathsName.toArray(String[]::new)))
                .tags(tags)
                .publishPercentiles(0.5, 0.75, 0.95, 0.999)
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    @Override
    public Timer timer(List<String> pathsName, List<Tag> tags, TaskEntity taskEntity) {
        List<Tag> allTags = ImmutableList.<Tag>builder()
                .addAll(tags)
                .add(buildAffinityGroupTag(taskEntity.getAffinityGroup()))
                .add(Tag.of("task_name", taskEntity.getTaskName()))
                .build();
        return timer(pathsName, allTags);
    }

    @Override
    public Counter counter(String... pathsName) {
        return counter(Lists.newArrayList(pathsName), List.of());
    }

    @Override
    public Counter counter(List<String> pathsName, List<Tag> tags) {
        return Counter.builder(buildName(pathsName))
                .tags(tags)
                .register(meterRegistry);
    }

    @Override
    public Counter counter(List<String> pathsName, List<Tag> tags, TaskEntity taskEntity) {
        List<Tag> allTags = ImmutableList.<Tag>builder()
                .addAll(tags)
                .add(buildAffinityGroupTag(taskEntity.getAffinityGroup()))
                .add(Tag.of("task_name", taskEntity.getTaskName()))
                .build();
        return counter(pathsName, allTags);
    }

    @Override
    public Gauge gauge(List<String> pathsName, Supplier<Number> supplier) {
        return gauge(pathsName, List.of(), supplier);
    }

    @Override
    public Gauge gauge(List<String> pathsName, List<Tag> tags, Supplier<Number> supplier) {
        return Gauge.builder(buildName(pathsName), supplier)
                .tags(tags)
                .register(meterRegistry);
    }

    private String buildName(List<String> pathNames) {
        return buildName(pathNames.toArray(String[]::new));
    }
}
