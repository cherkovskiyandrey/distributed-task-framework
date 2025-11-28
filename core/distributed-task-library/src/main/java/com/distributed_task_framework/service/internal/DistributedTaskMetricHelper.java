package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.utils.MetricHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Nullable;

import java.util.List;

public interface DistributedTaskMetricHelper extends MetricHelper {
    String UNDEFINED_VIRTUAL_QUEUE_NAME = "undefined";
    String DEFAULT_GROUP_TAG_NAME = "default";
    String TASK_NAME_TAG_NAME = "task_name";

    String virtualQueueStatName(@Nullable VirtualQueue virtualQueue);

    Tag buildVirtualQueueTag(@Nullable VirtualQueue virtualQueue);

    String affinityGroupStatName(@Nullable String value);

    Tag buildAffinityGroupTag(@Nullable String value);

    Timer timer(List<String> pathsName, List<Tag> tags, TaskEntity taskEntity);

    Counter counter(List<String> pathsName, List<Tag> tags, TaskEntity taskEntity);
}
