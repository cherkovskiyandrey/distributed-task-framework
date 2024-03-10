package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PlannerGroups;
import com.distributed_task_framework.service.internal.PlannerService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import com.distributed_task_framework.BaseMetricTest;
import com.distributed_task_framework.TaskPopulateAndVerify;

import java.util.List;

import static org.mockito.Mockito.when;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.allSet;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.allSetWithFixedWorker;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.deferred;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.noneSet;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.noneSetWithFixedWorker;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.withFixedWorker;
import static com.distributed_task_framework.TaskPopulateAndVerify.getAffinityGroup;
import static com.distributed_task_framework.TaskPopulateAndVerify.getNode;
import static com.distributed_task_framework.TaskPopulateAndVerify.getTaskName;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class VirtualQueueStatHelperTest extends BaseMetricTest {
    @MockBean(name = "virtualQueueManagerPlanner")
    PlannerService plannerService;
    @Autowired
    TaskPopulateAndVerify taskPopulateAndVerify;
    @Autowired
    VirtualQueueStatHelper virtualQueueStatHelper;

    @BeforeEach
    public void init() {
        super.init();
        when(plannerService.isActive()).thenReturn(true);
    }

    @Test
    void shouldDetectTasks() {
        //when
        prepareTasks();

        //do
        virtualQueueStatHelper.calculateAggregatedStat();

        //verify
        verifyAllTasksForMetric("planner.task.all");

        assertMetricToContain(
                "planner.task.notToPlan",
                METER_BY_GROUP_AND_NAME_AND_VIRTUAL_QUEUE,
                //in new
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(0), "new", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(0), "new", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(0), "new", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(1), "new", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(1), "new", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(1), "new", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(2), "new", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(2), "new", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(2), "new", 2),

                //in ready
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(0), "ready", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(0), "ready", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(0), "ready", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(1), "ready", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(1), "ready", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(1), "ready", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(2), "ready", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(2), "ready", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(2), "ready", 2),

                //in parked
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(0), "parked", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(0), "parked", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(0), "parked", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(1), "parked", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(1), "parked", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(1), "parked", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(2), "parked", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(2), "parked", 0),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(2), "parked", 2)
        );
    }

    @Test
    void shouldUpdateMoved() {
        //when
        List<TaskEntity> allTasks = prepareTasks();

        //do
        virtualQueueStatHelper.updateMoved(taskMapper.mapToShort(allTasks));

        //verify
        verifyAllTasksForMetric("planner.task.moved");
    }

    @Test
    void shouldUpdatePlannedTasks() {
        //when
        waitForNodeIsRegistered(
                TaskDef.privateTaskDef(getTaskName(0), String.class),
                TaskDef.privateTaskDef(getTaskName(1), String.class),
                TaskDef.privateTaskDef(getTaskName(2), String.class)
        );

        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(0, 1), allSetWithFixedWorker(3, getNode(0)),
                        Range.closedOpen(1, 2), noneSetWithFixedWorker(3, getNode(1)),
                        Range.closedOpen(2, 3), withFixedWorker(false, true, 3, getNode(2))
                )
        );
        var allAssignedTasks = taskPopulateAndVerify.populate(0, 18, VirtualQueue.READY, populationSpecs);

        //do
        virtualQueueStatHelper.updatePlannedTasks(taskMapper.mapToShort(allAssignedTasks));

        //verify
        assertMetricToContain(
                "planner.task.planned",
                METER_BY_GROUP_AND_NAME_AND_WORKER,
                Tuple.tuple(PlannerGroups.DEFAULT.getName(), getAffinityGroup(0), getTaskName(0), s(getNode(0)), 2),
                Tuple.tuple(PlannerGroups.DEFAULT.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(0), s(getNode(1)), 2),
                Tuple.tuple(PlannerGroups.DEFAULT.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(0), s(getNode(2)), 2),

                Tuple.tuple(PlannerGroups.DEFAULT.getName(), getAffinityGroup(0), getTaskName(1), s(getNode(0)), 2),
                Tuple.tuple(PlannerGroups.DEFAULT.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(1), s(getNode(1)), 2),
                Tuple.tuple(PlannerGroups.DEFAULT.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(1), s(getNode(2)), 2),

                Tuple.tuple(PlannerGroups.DEFAULT.getName(), getAffinityGroup(0), getTaskName(2), s(getNode(0)), 2),
                Tuple.tuple(PlannerGroups.DEFAULT.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(2), s(getNode(1)), 2),
                Tuple.tuple(PlannerGroups.DEFAULT.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(2), s(getNode(2)), 2)
        );
    }


    private void verifyAllTasksForMetric(String metricName) {
        assertMetricToContain(
                metricName,
                METER_BY_GROUP_AND_NAME_AND_VIRTUAL_QUEUE,
                //in new
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(0), "new", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(0), "new", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(0), "new", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(1), "new", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(1), "new", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(1), "new", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(2), "new", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(2), "new", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(2), "new", 2),

                //in ready
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(0), "ready", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(0), "ready", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(0), "ready", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(1), "ready", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(1), "ready", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(1), "ready", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(2), "ready", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(2), "ready", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(2), "ready", 2),

                //in parked
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(0), "parked", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(0), "parked", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(0), "parked", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(1), "parked", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(1), "parked", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(1), "parked", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(2), "parked", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(2), "parked", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(2), "parked", 2),

                //in deleted
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(0), "deleted", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(0), "deleted", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(0), "deleted", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(1), "deleted", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(1), "deleted", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(1), "deleted", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), MetricHelper.DEFAULT_GROUP_TAG_NAME, getTaskName(2), "deleted", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(1), getTaskName(2), "deleted", 2),
                Tuple.tuple(PlannerGroups.VQB_MANAGER.getName(), getAffinityGroup(2), getTaskName(2), "deleted", 2)
        );
    }

    private List<TaskEntity> prepareTasks() {
        waitForNodeIsRegistered(
                TaskDef.privateTaskDef(getTaskName(0), String.class),
                TaskDef.privateTaskDef(getTaskName(1), String.class),
                TaskDef.privateTaskDef(getTaskName(2), String.class)
        );

        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(0, 1), noneSet(3),
                        Range.closedOpen(1, 2), allSet(3),
                        Range.closedOpen(2, 3), deferred(3)
                )
        );
        List<TaskEntity> allTasks = Lists.newArrayList();
        allTasks.addAll(taskPopulateAndVerify.populate(0, 18, VirtualQueue.NEW, populationSpecs));
        allTasks.addAll(taskPopulateAndVerify.populate(0, 18, VirtualQueue.READY, populationSpecs));
        allTasks.addAll(taskPopulateAndVerify.populate(0, 18, VirtualQueue.PARKED, populationSpecs));
        allTasks.addAll(taskPopulateAndVerify.populate(0, 18, VirtualQueue.DELETED, populationSpecs));

        return allTasks;
    }
}