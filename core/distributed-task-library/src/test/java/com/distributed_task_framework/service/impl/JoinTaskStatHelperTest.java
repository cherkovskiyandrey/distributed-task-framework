package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.service.internal.PlannerGroups;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import com.distributed_task_framework.BaseMetricTest;

import java.util.List;
import java.util.Set;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class JoinTaskStatHelperTest extends BaseMetricTest {
    @Autowired
    JoinTaskStatHelper joinTaskStatHelper;

    @Test
    void shouldUpdateConcurrentChanged() {
        //when
        var taskEntity = buildBaseTaskEntityWithId("task-1");
        var taskEntityWithGroup = buildBaseTaskEntityWithId("task-2").toBuilder()
                .affinityGroup("test-grp-1")
                .build();

        //do
        joinTaskStatHelper.updateConcurrentChanged(
                List.of(taskEntity, taskEntityWithGroup),
                Set.of(taskEntity.getId(), taskEntityWithGroup.getId())
        );

        //verify
        metricTestHelper.assertMetricToContain(
                "planner.optLock.changed",
                METER_BY_GROUP_AND_NAME,
                Tuple.tuple(PlannerGroups.JOIN.getName(), "default", "task-1", 1),
                Tuple.tuple(PlannerGroups.JOIN.getName(), "test-grp-1", "task-2", 1)
        );
    }

    @Test
    void shouldUpdatePlannedTasks() {
        //when
        var taskEntity = buildBaseTaskEntityWithId("task-1");
        var taskEntityWithGroup = buildBaseTaskEntityWithId("task-2").toBuilder()
                .affinityGroup("test-grp-1")
                .build();

        //do
        joinTaskStatHelper.updatePlannedTasks(List.of(taskEntity, taskEntityWithGroup));

        //verify
        metricTestHelper.assertMetricToContain(
                "planner.task.planned",
                METER_BY_GROUP_AND_NAME,
                Tuple.tuple(PlannerGroups.JOIN.getName(), "default", "task-1", 1),
                Tuple.tuple(PlannerGroups.JOIN.getName(), "test-grp-1", "task-2", 1)
        );
    }
}