package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.model.NodeTaskActivity;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.TaskPopulateAndVerify;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class TaskVirtualQueueBasePlannerRepositoryTest extends BaseRepositoryTest {
    @Autowired
    @Qualifier("taskVirtualQueueBasePlannerRepositoryImpl")
    TaskVirtualQueueBasePlannerRepository repository;

    @Test
    void shouldGetCurrentAssignedTaskStat() {
        //when
        //10 * 2 = 20 tasks
        //10 * 2 = 20 workers
        var workerId = TaskPopulateAndVerify.getNode(0);
        var knownPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 10), TaskPopulateAndVerify.GenerationSpec.withWorker(2, workerId)
            )
        );
        taskPopulateAndVerify.populate(0, 20, VirtualQueue.READY, knownPopulationSpecs);
        Set<String> knownTaskNames = knownPopulationSpecs.stream()
            .flatMap(populationSpec -> populationSpec.getNameOfTasks().stream())
            .collect(Collectors.toSet());
        Set<UUID> knownNodes = Set.of(workerId);

        //deleted task have not been taken into account
        taskPopulateAndVerify.populate(0, 20, VirtualQueue.DELETED, knownPopulationSpecs);

        var unknownPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(10, 20), TaskPopulateAndVerify.GenerationSpec.withAutoAssignedWorker(2)
            )
        );
        taskPopulateAndVerify.populate(0, 20, VirtualQueue.READY, unknownPopulationSpecs);

        //do
        List<NodeTaskActivity> nodeTaskActivities = repository.currentAssignedTaskStat(knownNodes, knownTaskNames);

        //verify
        assertThat(nodeTaskActivities)
            .hasSize(2)
            .allMatch(nodeTaskActivity -> nodeTaskActivity.getNumber() == 10);
    }

    @Test
    void shouldFindPartitionStatToPlan() {
        //when
        Set<Partition> partitions = prepareDataToPlanInReadyQueue();

        //do
        var partitionStatToPlan = repository.findPartitionStatToPlan(
            Set.of(),
            partitions,
            20
        );

        //verify
        assertThat(partitionStatToPlan)
            .filteredOn(stat -> stat.getAffinityGroup() != null && Integer.parseInt(stat.getAffinityGroup()) < 5)
            .hasSize(10)
            .allMatch(stat -> stat.getNumber() == 20);

        assertThat(partitionStatToPlan)
            .filteredOn(stat -> stat.getAffinityGroup() == null || Integer.parseInt(stat.getAffinityGroup()) >= 5)
            .hasSize(5)
            .allMatch(stat -> stat.getNumber() == 10);
    }

    @Test
    void shouldLoadTasksToPlan() {
        //when
        Set<Partition> partitions = prepareDataToPlanInReadyQueue();
        Map<Partition, Integer> partitionLimits = partitions.stream()
            .collect(Collectors.toMap(
                Function.identity(),
                entity -> 1
            ));

        //do
        var shortTaskEntities = repository.loadTasksToPlan(Set.of(), partitionLimits);

        //verify
        assertThat(shortTaskEntities).hasSize(15);
    }

    private Set<Partition> prepareDataToPlanInReadyQueue() {
        //5 * 2 = 10 unique afg+taskName
        var knownPopulationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 5), TaskPopulateAndVerify.GenerationSpec.of(2)
            )
        );
        //200/10 = 20 tasks for each affinityGroup+taskName
        taskPopulateAndVerify.populate(0, 200, VirtualQueue.READY, knownPopulationSpecs);


        //5 unique afg+taskName
        var knownPopulationSpecsWithShortSize = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(5, 6), TaskPopulateAndVerify.GenerationSpec.oneWithoutAffinity(),
                Range.closedOpen(6, 10), TaskPopulateAndVerify.GenerationSpec.one()
            )
        );
        //50/5 = 10 tasks for each affinityGroup+taskName
        taskPopulateAndVerify.populate(0, 50, VirtualQueue.READY, knownPopulationSpecsWithShortSize);

        return ImmutableSet.<Partition>builder()
            .addAll(toPartitions(knownPopulationSpecs))
            .addAll(toPartitions(knownPopulationSpecsWithShortSize))
            .build();
    }
}