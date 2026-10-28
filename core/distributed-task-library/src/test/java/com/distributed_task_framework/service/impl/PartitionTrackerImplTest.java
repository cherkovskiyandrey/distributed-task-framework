package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.TaskPopulateAndVerify;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.persistence.entity.PartitionEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.oneWithAffinityGroupAndTaskName;
import static com.distributed_task_framework.TaskPopulateAndVerify.GenerationSpec.oneWithTaskNameAndWithoutAffinity;


@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class PartitionTrackerImplTest extends BaseSpringIntegrationTest {
    private static final String TASK_1 = "TASK_1";
    private static final String AFG_1 = "AFG_1";

    @Autowired
    PartitionTracker partitionTracker;
    @Autowired
    TaskPopulateAndVerify taskPopulateAndVerify;

    @Test
    void shouldReinit() {
        //when
        var spec = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
            Range.closedOpen(0, 10), TaskPopulateAndVerify.GenerationSpec.one())
        );
        var alreadyInReady = taskPopulateAndVerify.populate(0, 5, VirtualQueue.READY, spec);

        //do
        partitionTracker.reinit();

        //verify
        this.verifyRegisteredPartitionFromTask(alreadyInReady);
    }

    //affinityGroupTaskNameTimeWindow = 5 sec for test ctx
    @Test
    void shouldTrack() {
        //when
        setFixedTime(100);
        var partitions = Set.of(
            Partition.builder().taskName(TASK_1).build(),
            Partition.builder().affinityGroup(AFG_1).taskName(TASK_1).build()
        );

        //do
        partitionTracker.track(partitions);

        //verify
        verifyRegisteredPartitionEntities(partitionMapper.asEntities(partitions, 100L / 5));
    }

    @Test
    void shouldReturnAll() {
        //when
        var partitions = List.of(
            createPartition(null, "1", 1L),
            createPartition("1", "1", 1L),
            createPartition(null, "1", 2L),
            createPartition("1", "1", 2L)
        );
        partitionRepository.saveAll(partitions);

        //do
        var actualPartitions = partitionTracker.getAll();

        //verify
        verifyRegisteredPartition(actualPartitions);
    }

    @Test
    void shouldGcIfNecessary() {
        //when
        setFixedTime(100);
        long currentTimeBucket = 100 / 5;
        partitionTracker.reinit();

        var spec = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 1), oneWithTaskNameAndWithoutAffinity("exist_in_ready_queue"),
                Range.closedOpen(1, 2), oneWithAffinityGroupAndTaskName("exist_in_ready_queue", "exist_in_ready_queue")
            )
        );
        taskPopulateAndVerify.populate(0, 2, VirtualQueue.READY, spec);

        var expectedToRemainedFromActiveQueue = List.of(
            createPartition(null, "exist_in_ready_queue", currentTimeBucket - 2),
            createPartition("exist_in_ready_queue", "exist_in_ready_queue", currentTimeBucket - 2)
        );
        var expectedToDeleted = List.of(
            createPartition(null, "1", currentTimeBucket - 2),
            createPartition("1", "2", currentTimeBucket - 2)
        );
        var expectedBeRemained = List.of(
            createPartition(null, "3", currentTimeBucket - 1),
            createPartition("2", "4", currentTimeBucket - 1),
            createPartition(null, "5", currentTimeBucket),
            createPartition("3", "6", currentTimeBucket)
        );
        partitionRepository.saveAll(expectedToRemainedFromActiveQueue);
        partitionRepository.saveAll(expectedToDeleted);
        partitionRepository.saveAll(expectedBeRemained);


        //do
        partitionTracker.gcIfNecessary();

        //verify
        var expectedPartitions = ImmutableList.<PartitionEntity>builder()
            .addAll(expectedToRemainedFromActiveQueue)
            .addAll(expectedBeRemained)
            .build();
        verifyRegisteredPartition(partitionMapper.fromEntities(expectedPartitions));
    }

    @Test
    void shouldGcIfNecessaryOnlyOnceInTimeWindow() {
        //when
        setFixedTime(100);
        long currentTimeBucket = 100 / 5;
        partitionTracker.reinit();

        var expectedToDeleted = List.of(
            createPartition("1", "1", currentTimeBucket - 2)
        );
        var expectedBeRemained = List.of(
            createPartition("2", "2", currentTimeBucket - 1),
            createPartition("3", "3", currentTimeBucket)
        );
        partitionRepository.saveAll(expectedToDeleted);
        partitionRepository.saveAll(expectedBeRemained);

        //do
        partitionTracker.gcIfNecessary();

        //verify
        var expectedPartitions = ImmutableList.<PartitionEntity>builder()
            .addAll(expectedBeRemained)
            .build();
        verifyRegisteredPartition(partitionMapper.fromEntities(expectedPartitions));

        //when
        //syntetick case in order to check guard in gcIfNecessary() in currentTimeWindow
        expectedToDeleted = List.of(
            createPartition("100", "100", currentTimeBucket - 2)
        );
        partitionRepository.saveAll(expectedToDeleted);

        //do
        partitionTracker.gcIfNecessary();

        //verify
        expectedPartitions = ImmutableList.<PartitionEntity>builder()
            .addAll(expectedBeRemained)
            .addAll(expectedToDeleted)
            .build();
        verifyRegisteredPartition(partitionMapper.fromEntities(expectedPartitions));

        //when
        setFixedTime(100 + 6);

        //do
        partitionTracker.gcIfNecessary();

        //verify
        expectedBeRemained = List.of(
            createPartition("3", "3", currentTimeBucket)
        );
        expectedPartitions = ImmutableList.<PartitionEntity>builder()
            .addAll(expectedBeRemained)
            .build();
        verifyRegisteredPartition(partitionMapper.fromEntities(expectedPartitions));
    }

    @Test
    void shouldCompactIfNecessary() {
        //when
        setFixedTime(100);
        long currentTimeBucket = 100 / 5;
        partitionTracker.reinit();

        var expectedToBeRemained = List.of(
            createPartition(null, "1", currentTimeBucket - 1),
            createPartition("1", "2", currentTimeBucket - 1),
            createPartition(null, "1", currentTimeBucket - 1),
            createPartition("1", "2", currentTimeBucket - 1),

            // original
            createPartition(null, "1", currentTimeBucket),
            createPartition("1", "2", currentTimeBucket)
        );
        var expectedBeDeleted = List.of(
            createPartition(null, "1", currentTimeBucket),
            createPartition("1", "2", currentTimeBucket),
            createPartition(null, "1", currentTimeBucket),
            createPartition("1", "2", currentTimeBucket),
            createPartition(null, "1", currentTimeBucket),
            createPartition("1", "2", currentTimeBucket)
        );
        partitionRepository.saveAll(expectedToBeRemained);
        partitionRepository.saveAll(expectedBeDeleted);

        //do
        partitionTracker.compactIfNecessary();

        //verify
        verifyRegisteredPartition(partitionMapper.fromEntities(expectedToBeRemained));
    }

    //todo
    @Test
    void shouldCompactIfNecessaryOnceInTimeWindow() {
        //when
        setFixedTime(100);
        long currentTimeBucket = 100 / 5;
        partitionTracker.reinit();

        var expectedToBeRemained = List.of(
            // original
            createPartition("1", "2", currentTimeBucket)
        );
        var expectedBeDeleted = List.of(
            createPartition("1", "2", currentTimeBucket)
        );
        partitionRepository.saveAll(expectedToBeRemained);
        partitionRepository.saveAll(expectedBeDeleted);

        //do
        partitionTracker.compactIfNecessary();

        //verify
        verifyRegisteredPartition(partitionMapper.fromEntities(expectedToBeRemained));

        //when
        setFixedTime(100 + 1);
        expectedToBeRemained = List.of(
            // original
            createPartition("1", "2", currentTimeBucket),
            createPartition("1", "2", currentTimeBucket)
        );
        partitionRepository.saveAll(expectedToBeRemained);

        //do
        partitionTracker.compactIfNecessary();

        //verify
        verifyRegisteredPartition(partitionMapper.fromEntities(expectedToBeRemained));
    }

    private void verifyRegisteredPartitionEntities(Collection<PartitionEntity> expectedPartitions) {
        Assertions.assertThat(partitionRepository.filterExisted(expectedPartitions))
            .containsExactlyInAnyOrderElementsOf(expectedPartitions);
    }
}