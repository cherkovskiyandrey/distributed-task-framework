package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.TaskPopulateAndVerify;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class PartitionTrackerRepositoryTest extends BaseRepositoryTest {
    @Autowired
    @Qualifier("partitionTrackerRepositoryImpl")
    PartitionTrackerRepository partitionTrackerRepository;

    @Test
    void shouldReturnActivePartitions() {
        //when
        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.withoutAffinity(2),
                Range.closedOpen(1, 3), TaskPopulateAndVerify.GenerationSpec.of(2)
            )
        );
        taskPopulateAndVerify.populate(0, 100, VirtualQueue.READY, populationSpecs);

        //do
        Set<Partition> partitionEntities = partitionTrackerRepository.activePartitions();

        //verify
        var expectedResult = List.of(
            Partition.builder()
                .affinityGroup(null)
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + 0)
                .build(),
            Partition.builder()
                .affinityGroup(null)
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + 1)
                .build(),

            Partition.builder()
                .affinityGroup("1")
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + 0)
                .build(),
            Partition.builder()
                .affinityGroup("1")
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + 1)
                .build(),

            Partition.builder()
                .affinityGroup("2")
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + 0)
                .build(),
            Partition.builder()
                .affinityGroup("2")
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + 1)
                .build()
        );
        assertThat(partitionEntities).containsAll(expectedResult);
    }

    @Test
    void shouldFilterInReadyVirtualQueue() {
        //when
        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.oneWithoutAffinity(),
                Range.closedOpen(2, 3), TaskPopulateAndVerify.GenerationSpec.one()
            )
        );
        taskPopulateAndVerify.populate(0, 100, VirtualQueue.READY, populationSpecs);

        Set<Partition> checkedEntities = Set.of(
            Partition.builder()
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + "0")
                .build(),
            Partition.builder()
                .affinityGroup("2")
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + "0")
                .build(),
            Partition.builder()
                .affinityGroup("2")
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + "1")
                .build()
        );

        //do
        var actualFilteredEntities = partitionTrackerRepository.filterInReadyVirtualQueue(checkedEntities);

        //verify
        assertThat(actualFilteredEntities).containsExactlyInAnyOrder(
            Partition.builder()
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + "0")
                .build(),
            Partition.builder()
                .affinityGroup("2")
                .taskName(TaskPopulateAndVerify.TASK_PREFIX + "0")
                .build()
        );
    }
}