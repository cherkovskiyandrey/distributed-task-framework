package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;

public interface MetricHelper {
    String UNDEFINED_VIRTUAL_QUEUE_NAME = "undefined";
    String DEFAULT_GROUP_TAG_NAME = "default";
    String TASK_NAME_TAG_NAME = "task_name";

    String virtualQueueStatName(@Nullable VirtualQueue virtualQueue);

    Tag buildVirtualQueueTag(@Nullable VirtualQueue virtualQueue);

    String affinityGroupStatName(@Nullable String value);

    Tag buildAffinityGroupTag(@Nullable String value);

    String buildName(String... paths);

    Timer timer(String... pathsName);

    Timer timer(List<String> pathsName, List<Tag> tags);

    Timer timer(List<String> pathsName, List<Tag> tags, TaskEntity taskEntity);

    Counter counter(String... pathsName);

    Counter counter(List<String> pathsName, List<Tag> tags);

    Counter counter(List<String> pathsName, List<Tag> tags, TaskEntity taskEntity);

    Gauge gauge(List<String> pathsName, Supplier<Number> supplier);

    Gauge gauge(List<String> pathsName, List<Tag> tags, Supplier<Number> supplier);
}
