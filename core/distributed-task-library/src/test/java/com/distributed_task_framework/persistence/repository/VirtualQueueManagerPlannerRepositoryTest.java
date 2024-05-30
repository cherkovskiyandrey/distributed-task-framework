package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.model.AffinityGroupStat;
import com.distributed_task_framework.model.AffinityGroupWrapper;
import com.distributed_task_framework.TaskPopulateAndVerify;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class VirtualQueueManagerPlannerRepositoryTest extends BaseRepositoryTest {
    @Autowired
    @Qualifier("virtualQueueManagerPlannerRepositoryImpl")
    VirtualQueueManagerPlannerRepository repository;


    @Test
    void shouldReturnMaxCreatedDateInNewVirtualQueue() {
        //when
        setFixedTime();
        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask()
                )
        );
        taskPopulateAndVerify.populate(0, 100, VirtualQueue.NEW, populationSpecs);

        setFixedTime(Duration.ofHours(1).toSeconds());
        taskPopulateAndVerify.populate(0, 1, VirtualQueue.NEW, populationSpecs);

        //do
        Optional<LocalDateTime> maxCreatedDateOpt = repository.maxCreatedDateInNewVirtualQueue();

        //verify
        assertThat(maxCreatedDateOpt)
                .isPresent()
                .get()
                .isEqualTo(LocalDateTime.now(clock));
    }

    @Test
    void shouldGetAffinityGroupsInNewVirtualQueue() {
        //when
        Duration firstPoint = Duration.ZERO;
        Duration secondPoint = Duration.ofHours(1);
        Duration thirdPoint = Duration.ofHours(2);
        Duration checkPoint = Duration.ofHours(2);
        Duration overlap = Duration.ofHours(1);

        setFixedTime(firstPoint.toSeconds());
        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask()
                )
        );
        taskPopulateAndVerify.populate(0, 100, VirtualQueue.NEW, populationSpecs);

        setFixedTime(secondPoint.toSeconds());
        populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(1, 2), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask()
                )
        );
        taskPopulateAndVerify.populate(0, 100, VirtualQueue.NEW, populationSpecs);

        setFixedTime(thirdPoint.toSeconds());
        populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(2, 3), TaskPopulateAndVerify.GenerationSpec.noneSetAndOneTask(),
                        Range.closedOpen(3, 4), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask()
                )
        );
        taskPopulateAndVerify.populate(0, 100, VirtualQueue.NEW, populationSpecs);

        //do
        setFixedTime(checkPoint.toSeconds());
        var affinityGroupWrappers = repository.affinityGroupsInNewVirtualQueue(LocalDateTime.now(clock), overlap);

        //verify
        List<AffinityGroupWrapper> expectedResult = List.of(
                new AffinityGroupWrapper("1"),
                new AffinityGroupWrapper(), //2 is null
                new AffinityGroupWrapper("3")
        );
        assertThat(affinityGroupWrappers).containsAll(expectedResult);
    }

    @Test
    void shouldReturnAffinityGroupInNewVirtualQueueStat() {
        //when
        final int limit = 10;
        setFixedTime();
        //total=10 affinityGroups
        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(0, 6), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask(),

                        //-- affinityGroup = null for both records
                        Range.closedOpen(6, 7), TaskPopulateAndVerify.GenerationSpec.oneTask(false, true),
                        Range.closedOpen(7, 8), TaskPopulateAndVerify.GenerationSpec.noneSetAndOneTask(),
                        //--------------------------

                        Range.closedOpen(8, 11), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask()
                )
        );

        //220/11(groups) = 20 tasks in each
        //for affinityGroups: 20 task in each and 60 in affinityGroups=null
        taskPopulateAndVerify.populate(0, 220, VirtualQueue.NEW, populationSpecs);

        Set<AffinityGroupWrapper> affinityGroupWrappers = populationSpecs.stream()
                .map(populationSpec -> new AffinityGroupWrapper(populationSpec.getAffinityGroup()))
                .collect(Collectors.toSet());

        setFixedTime(10_000); //>taskPopulate.populate(1000...

        //do
        var affinityGroupStats = repository.affinityGroupInNewVirtualQueueStat(affinityGroupWrappers, limit);

        //verify
        assertThat(affinityGroupStats).anyMatch(affinityGroupStat -> affinityGroupStat.getAffinityGroupName() == null);
        assertThat(Lists.newArrayList(affinityGroupStats))
                .allMatch(affinityGroupStat -> limit == affinityGroupStat.getNumber());
    }

    @Test
    void shouldMoveNewToReady() {
        //when
        final int limit = 6;
        //setFixedTime();
        //total=10 affinityGroups
        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 6), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask(),

                //-- affinityGroup = null for all records
                Range.closedOpen(6, 7), TaskPopulateAndVerify.GenerationSpec.oneTask(false, true),
                Range.closedOpen(7, 8), TaskPopulateAndVerify.GenerationSpec.oneTask(false, true),
                Range.closedOpen(8, 9), TaskPopulateAndVerify.GenerationSpec.noneSetAndOneTask(),
                //--------------------------

                Range.closedOpen(9, 12), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask()
        ));
        //100/10(groups) = 10 tasks for each affinityGroup
        taskPopulateAndVerify.populate(0, 3, VirtualQueue.READY, populationSpecs);
        taskPopulateAndVerify.populate(3, 6, VirtualQueue.PARKED, populationSpecs);
        taskPopulateAndVerify.populate(6, 7, VirtualQueue.READY, populationSpecs);
        taskPopulateAndVerify.populate(7, 8, VirtualQueue.PARKED, populationSpecs);
        taskPopulateAndVerify.populate(8, 9, VirtualQueue.READY, populationSpecs);

        var newTaskEntities = taskPopulateAndVerify.populate(0, 100, VirtualQueue.NEW, populationSpecs);

        //setFixedTime(1_000); //>taskPopulate.populate(100...

        //do
        var movedShortTaskEntities = repository.moveNewToReady(toAffinityGroupStat(populationSpecs, limit));

        //verify
        TaskPopulateAndVerify.VerifyVirtualQueueContext baseVerifyCtx = TaskPopulateAndVerify.VerifyVirtualQueueContext.builder()
                .populationSpecRange(Range.closedOpen(0, 6))
                .expectedVirtualQueueByRange(Map.of(
                        Range.closedOpen(0, limit),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.PARKED)
                ))
                .populationSpecs(populationSpecs)
                .affectedTaskEntities(newTaskEntities)
                .movedShortTaskEntities(movedShortTaskEntities)
                .build();

        taskPopulateAndVerify.verifyVirtualQueue(baseVerifyCtx);
        taskPopulateAndVerify.verifyVirtualQueue(baseVerifyCtx.toBuilder()
                .populationSpecRange(Range.closedOpen(6, 8))
                .expectedVirtualQueueByRange(Map.of(
                        //3 because we have limit = 6, 1 affinity group (default) and 3 type of tasks
                        Range.closedOpen(0, limit / 3),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.PARKED)
                ))
                .build()
        );
        taskPopulateAndVerify.verifyVirtualQueue(baseVerifyCtx.toBuilder()
                .populationSpecRange(Range.closedOpen(8, 9))
                .expectedVirtualQueueByRange(Map.of(
                        //3 because we have limit = 6, 1 affinity group (default) and 3 type of tasks
                        Range.closedOpen(0, limit / 3),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.READY)
                ))
                .build()
        );
        taskPopulateAndVerify.verifyVirtualQueue(baseVerifyCtx.toBuilder()
                .populationSpecRange(Range.closedOpen(9, 11))
                .expectedVirtualQueueByRange(Map.of(
                        Range.closedOpen(0, 1),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.READY),
                        Range.closedOpen(1, limit),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.PARKED)
                ))
                .build()
        );
    }

    @Test
    void shouldMoveParkedToReady() {
        //when
        final int limit = 11;
        setFixedTime();
        //total=10 affinityGroups
        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                        Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask(),
                        Range.closedOpen(1, 2), TaskPopulateAndVerify.GenerationSpec.oneTask(false, true),
                        Range.closedOpen(2, 3), TaskPopulateAndVerify.GenerationSpec.oneTask(false, true),
                        Range.closedOpen(3, 10), TaskPopulateAndVerify.GenerationSpec.allSetAndOneTask()
                )
        );
        taskPopulateAndVerify.populate(0, 2, VirtualQueue.READY, populationSpecs);
        //100/10(populationSpecs) = 10 tasks for each group
        var parkedTaskEntities = taskPopulateAndVerify.populate(0, 100, VirtualQueue.PARKED, populationSpecs);
        taskPopulateAndVerify.populate(0, 10, VirtualQueue.DELETED, populationSpecs);

        List<TaskPopulateAndVerify.PopulationSpec> populationSpecWithoutAffinity = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.noneSetAndOneTask()
        ));
        var stillDeletedTaskEntities = taskPopulateAndVerify.populate(0, 1, VirtualQueue.DELETED, populationSpecWithoutAffinity);

        setFixedTime(1_000); //>taskPopulate.populate(100...

        //do
        var movedShortTaskEntities = repository.moveParkedToReady(limit);

        //verify
        TaskPopulateAndVerify.VerifyVirtualQueueContext baseVerifyCtx = TaskPopulateAndVerify.VerifyVirtualQueueContext.builder()
                .populationSpecs(populationSpecs)
                .affectedTaskEntities(parkedTaskEntities)
                .movedShortTaskEntities(movedShortTaskEntities)
                .build();

        TaskPopulateAndVerify.VerifyVirtualQueueContext stillParkedVerifyCtx = baseVerifyCtx.toBuilder()
                .populationSpecRange(Range.closedOpen(0, 2))
                .expectedVirtualQueueByRange(Map.of(
                        Range.closedOpen(0, 10),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.untouched(VirtualQueue.PARKED)
                ))
                .build();
        taskPopulateAndVerify.verifyVirtualQueue(stillParkedVerifyCtx);

        TaskPopulateAndVerify.VerifyVirtualQueueContext movedToReadyVerifyCtx = baseVerifyCtx.toBuilder()
                .populationSpecRange(Range.closedOpen(3, 10))
                .expectedVirtualQueueByRange(Map.of(
                        Range.closedOpen(0, 1),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.READY),

                        Range.closedOpen(1, 10),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.untouched(VirtualQueue.PARKED)
                ))
                .build();
        taskPopulateAndVerify.verifyVirtualQueue(movedToReadyVerifyCtx);

        TaskPopulateAndVerify.VerifyVirtualQueueContext stillDeletedVerifyCtx = baseVerifyCtx.toBuilder()
                .populationSpecs(populationSpecWithoutAffinity)
                .affectedTaskEntities(stillDeletedTaskEntities)
                .populationSpecRange(Range.closedOpen(0, 1))
                .expectedVirtualQueueByRange(Map.of(
                        Range.closedOpen(0, 1),
                        TaskPopulateAndVerify.ExpectedVirtualQueue.untouched(VirtualQueue.DELETED)
                ))
                .build();
        taskPopulateAndVerify.verifyVirtualQueue(stillDeletedVerifyCtx);
    }

    private Set<AffinityGroupStat> toAffinityGroupStat(List<TaskPopulateAndVerify.PopulationSpec> affinityGroupAndAffinities, int limit) {
        return affinityGroupAndAffinities.stream()
                .map(populationSpec -> AffinityGroupStat.builder()
                        .affinityGroupName(populationSpec.getAffinityGroup())
                        .number(limit)
                        .build()
                )
                .collect(Collectors.toSet());
    }
}