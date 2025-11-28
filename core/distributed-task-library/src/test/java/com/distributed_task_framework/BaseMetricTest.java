package com.distributed_task_framework;

import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.utils.MetricTestHelper;
import io.micrometer.core.instrument.Meter;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.function.Function;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public class BaseMetricTest extends BaseSpringIntegrationTest {
    @Autowired
    MetricTestHelper metricTestHelper;

    protected TaskEntity buildBaseTaskEntity(String name) {
        return TaskEntity.builder()
            .taskName(name)
            .virtualQueue(VirtualQueue.NEW)
            .workflowId(UUID.randomUUID())
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .build();
    }

    protected TaskEntity buildBaseTaskEntityWithId(String name) {
        return buildBaseTaskEntity(name).toBuilder()
            .id(UUID.randomUUID())
            .build();
    }

    @SuppressWarnings("unchecked")
    protected static final Function<Meter, ?>[] METER_DEFINITION_BY_GROUP_AND_NODE_NAME = new Function[]{
        meter -> ((Meter) meter).getId().getTag("group"),
        meter -> ((Meter) meter).getId().getTag("nodeId"),
        meter -> (int) ((Meter) meter).measure().iterator().next().getValue()
    };

    @SuppressWarnings("unchecked")
    protected static final Function<Meter, ?>[] METER_BY_GROUP_AND_NODE_NAME = new Function[]{
        meter -> ((Meter) meter).getId().getTag("group"),
        meter -> ((Meter) meter).getId().getTag("nodeId"),
        meter -> (int) ((Meter) meter).measure().iterator().next().getValue()
    };

    @SuppressWarnings("unchecked")
    protected static final Function<Meter, ?>[] METER_BY_GROUP_AND_NAME = new Function[]{
        meter -> ((Meter) meter).getId().getTag("group"),
        meter -> ((Meter) meter).getId().getTag("affinity_group"),
        meter -> ((Meter) meter).getId().getTag("task_name"),
        meter -> (int) ((Meter) meter).measure().iterator().next().getValue()
    };

    @SuppressWarnings("unchecked")
    protected static final Function<Meter, ?>[] METER_BY_GROUP_AND_NAME_AND_VIRTUAL_QUEUE = new Function[]{
        meter -> ((Meter) meter).getId().getTag("group"),
        meter -> ((Meter) meter).getId().getTag("affinity_group"),
        meter -> ((Meter) meter).getId().getTag("task_name"),
        meter -> ((Meter) meter).getId().getTag("virtualQueue"),
        meter -> (int) ((Meter) meter).measure().iterator().next().getValue()
    };

    @SuppressWarnings("unchecked")
    protected static final Function<Meter, ?>[] METER_BY_GROUP_AND_NAME_AND_WORKER = new Function[]{
        meter -> ((Meter) meter).getId().getTag("group"),
        meter -> ((Meter) meter).getId().getTag("affinity_group"),
        meter -> ((Meter) meter).getId().getTag("task_name"),
        meter -> ((Meter) meter).getId().getTag("worker_id"),
        meter -> (int) ((Meter) meter).measure().iterator().next().getValue()
    };

    protected static String s(UUID nodeId) {
        return nodeId.toString();
    }
}
