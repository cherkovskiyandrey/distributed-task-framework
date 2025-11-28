package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.service.internal.DistributedTaskMetricHelper;
import com.distributed_task_framework.utils.MetricHelperImpl;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class DistributedTaskMetricHelperImpl extends MetricHelperImpl implements DistributedTaskMetricHelper {

    public DistributedTaskMetricHelperImpl(MeterRegistry meterRegistry) {
        super(meterRegistry);
    }

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
    public Timer timer(List<String> pathsName, List<Tag> tags, TaskEntity taskEntity) {
        List<Tag> allTags = ImmutableList.<Tag>builder()
            .addAll(tags)
            .add(buildAffinityGroupTag(taskEntity.getAffinityGroup()))
            .add(Tag.of("task_name", taskEntity.getTaskName()))
            .build();
        return timer(pathsName, allTags);
    }

    @Override
    public Counter counter(List<String> pathsName, List<Tag> tags, TaskEntity taskEntity) {
        List<Tag> allTags = ImmutableList.<Tag>builder()
            .addAll(tags)
            .add(buildAffinityGroupTag(taskEntity.getAffinityGroup()))
            .add(Tag.of(TASK_NAME_TAG_NAME, taskEntity.getTaskName()))
            .build();
        return counter(pathsName, allTags);
    }
}
