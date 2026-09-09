package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.TaskPopulateAndVerify;
import com.distributed_task_framework.model.AffinityGroupAndAffinity;
import com.distributed_task_framework.model.AffinityGroupStat;
import com.distributed_task_framework.model.AffinityGroupWrapper;
import com.distributed_task_framework.persistence.entity.TaskEntity;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.distributed_task_framework.TaskPopulateAndVerify.getAffinityGroup;
import static java.time.LocalDateTime.now;
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
                Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.one()
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
            .isEqualTo(now(clock));
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
                Range.closedOpen(0, 1), TaskPopulateAndVerify.GenerationSpec.one()
            )
        );
        taskPopulateAndVerify.populate(0, 100, VirtualQueue.NEW, populationSpecs);

        setFixedTime(secondPoint.toSeconds());
        populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(1, 2), TaskPopulateAndVerify.GenerationSpec.one()
            )
        );
        taskPopulateAndVerify.populate(0, 100, VirtualQueue.NEW, populationSpecs);

        setFixedTime(thirdPoint.toSeconds());
        populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(2, 3), TaskPopulateAndVerify.GenerationSpec.oneWithoutAffinity(),
                Range.closedOpen(3, 4), TaskPopulateAndVerify.GenerationSpec.one()
            )
        );
        taskPopulateAndVerify.populate(0, 100, VirtualQueue.NEW, populationSpecs);

        //do
        setFixedTime(checkPoint.toSeconds());
        var affinityGroupWrappers = repository.affinityGroupsInNewVirtualQueue(now(clock), overlap);

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
                Range.closedOpen(0, 6), TaskPopulateAndVerify.GenerationSpec.one(),
                Range.closedOpen(6, 7), TaskPopulateAndVerify.GenerationSpec.oneWithoutAffinity(),
                Range.closedOpen(7, 10), TaskPopulateAndVerify.GenerationSpec.one()
            )
        );

        //200/10(groups) = 20 tasks in each
        taskPopulateAndVerify.populate(0, 200, VirtualQueue.NEW, populationSpecs);

        Set<AffinityGroupWrapper> affinityGroupWrappers = populationSpecs.stream()
            .map(populationSpec -> new AffinityGroupWrapper(populationSpec.getAffinityGroup()))
            .collect(Collectors.toSet());

        setFixedTime(10_000); //>taskPopulate.populate(1000...

        //do
        var affinityGroupStats = repository.affinityGroupInNewVirtualQueueStat(affinityGroupWrappers, limit);

        //verify
        Map<String, Integer> affinityGroupToNumber = affinityGroupStats.stream()
            .filter(affinityGroupStat -> affinityGroupStat.getAffinityGroup() != null)
            .collect(Collectors.groupingBy(
                AffinityGroupStat::getAffinityGroup,
                Collectors.summingInt(AffinityGroupStat::getNumber)
            ));

        IntStream.range(0, 6)
            .forEach(group -> assertThat(affinityGroupToNumber.getOrDefault(getAffinityGroup(group), 0)).isEqualTo(limit));

        IntStream.range(7, 10)
            .forEach(group -> assertThat(affinityGroupToNumber.getOrDefault(getAffinityGroup(group), 0)).isEqualTo(limit));

        assertThat(affinityGroupStats).anyMatch(affinityGroupStat -> affinityGroupStat.getAffinityGroup() == null);

        assertThat(Lists.newArrayList(affinityGroupStats))
            .allMatch(affinityGroupStat -> limit == affinityGroupStat.getNumber());
    }

    @Test
    void shouldGetTasksFromNew() {
        //when
        final int limit = 5;
        var populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 2), TaskPopulateAndVerify.GenerationSpec.one(),
                Range.closedOpen(2, 4), TaskPopulateAndVerify.GenerationSpec.oneWithoutAffinity(),
                Range.closedOpen(4, 6), TaskPopulateAndVerify.GenerationSpec.one(),
                Range.closedOpen(6, 7), TaskPopulateAndVerify.GenerationSpec.oneWithFixedWorkflow(LocalDateTime.now(clock))
            )
        );
        taskPopulateAndVerify.populate(0, 1, VirtualQueue.READY, populationSpecs);
        taskPopulateAndVerify.populate(1, 2, VirtualQueue.PARKED, populationSpecs);

        taskPopulateAndVerify.populate(2, 3, VirtualQueue.READY, populationSpecs);
        taskPopulateAndVerify.populate(3, 4, VirtualQueue.PARKED, populationSpecs);

        //7 groups => 70/7 => 10 tasks in each group
        var newTaskEntities = taskPopulateAndVerify.populate(0, 70, VirtualQueue.NEW, populationSpecs);

        //do
        var idVersionWithVirtualQueues = repository.getTasksFromNew(toAffinityGroupStat(populationSpecs, limit));

        //verify
        var movedShortTaskEntities = idVersionWithVirtualQueues.stream()
            .map(idVersionWithVirtualQueue -> taskRepository.findById(idVersionWithVirtualQueue.getId())
                .map(taskEntity -> taskEntity.toBuilder()
                    .virtualQueue(idVersionWithVirtualQueue.getVirtualQueue())
                    .build()
                )
                .orElseThrow()
            )
            .map(taskRepository::saveOrUpdate)
            .map(taskMapper::mapToShort)
            .toList();

        TaskPopulateAndVerify.VerifyVirtualQueueContext baseVerifyCtx = TaskPopulateAndVerify.VerifyVirtualQueueContext.builder()
            .populationSpecRange(Range.closedOpen(0, 2)) // group range
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, limit), // tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.PARKED)
            ))
            .populationSpecs(populationSpecs)
            .affectedTaskEntities(newTaskEntities)
            .movedShortTaskEntities(movedShortTaskEntities)
            .build();

        taskPopulateAndVerify.verifyVirtualQueue(baseVerifyCtx);

        taskPopulateAndVerify.verifyVirtualQueue(baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(2, 4)) // group range
            .expectedVirtualQueueByRange(Map.of(
                //we have limit = 6, 1 affinity group (default) and 2 type of tasks => limit / 2
                Range.closedOpen(0, limit / 2),
                TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.READY)
            ))
            .build()
        );

        taskPopulateAndVerify.verifyVirtualQueue(baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(4, 6)) // group range
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 1), // tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.READY),
                Range.closedOpen(1, limit), // tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.PARKED)
            ))
            .build()
        );

        taskPopulateAndVerify.verifyVirtualQueue(baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(6, 7)) // group range
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 10), // all task because of workflowId is the same
                TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.READY)
            ))
            .build()
        );
    }

    @Test
    void shouldMoveNewToReadyAndParked() {
        //when
        var populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 10), TaskPopulateAndVerify.GenerationSpec.one()
            )
        );
        var parkedTaskEntities = taskPopulateAndVerify.populate(0, 10, VirtualQueue.NEW, populationSpecs);

        var shouldMovedToReadyIdVersion = parkedTaskEntities.stream()
            .limit(4)
            .map(taskEntity -> taskEntity.toBuilder()
                .virtualQueue(VirtualQueue.READY)
                .build()
            )
            .map(idVersionMapper::mapToIdVersionWithVirtualQueue);
        var shouldMovedToParkedIdVersion = parkedTaskEntities.stream()
            .skip(4)
            .limit(4)
            .map(taskEntity -> taskEntity.toBuilder()
                .virtualQueue(VirtualQueue.PARKED)
                .build()
            )
            .map(idVersionMapper::mapToIdVersionWithVirtualQueue);
        var shouldNotMovedIdVersions = parkedTaskEntities.stream()
            .skip(8)
            .map(taskEntity -> taskEntity.toBuilder()
                .version(taskEntity.getVersion() + 1)
                .virtualQueue(VirtualQueue.READY)
                .build()
            )
            .map(idVersionMapper::mapToIdVersionWithVirtualQueue);
        var newToReadyAndParkedRequest = Stream.concat(
            Stream.concat(
                shouldMovedToReadyIdVersion,
                shouldMovedToParkedIdVersion
            ),
            shouldNotMovedIdVersions
        ).toList();

        //do
        var movedShortTaskEntities = repository.moveNewToReadyAndParked(newToReadyAndParkedRequest);

        //verify
        TaskPopulateAndVerify.VerifyVirtualQueueContext baseVerifyCtx = TaskPopulateAndVerify.VerifyVirtualQueueContext.builder()
            .populationSpecs(populationSpecs)
            .affectedTaskEntities(parkedTaskEntities)
            .movedShortTaskEntities(movedShortTaskEntities)
            .build();

        var movedToReadyVerifyCtx = baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(0, 4)) //range of groups
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 1), //rage on tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.READY)
            ))
            .build();
        taskPopulateAndVerify.verifyVirtualQueue(movedToReadyVerifyCtx);

        var movedToParkedVerifyCtx = baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(4, 8)) //range of groups
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 1), //rage on tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.PARKED)
            ))
            .build();
        taskPopulateAndVerify.verifyVirtualQueue(movedToParkedVerifyCtx);

        var stillNewVerifyCtx = baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(8, 10)) //range of groups
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 1), //rage on tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.untouched(VirtualQueue.NEW)
            ))
            .build();
        taskPopulateAndVerify.verifyVirtualQueue(stillNewVerifyCtx);
    }

    @Test
    void shouldReadyToHardDelete() {
        //when
        setFixedTime(1_000);
        //total=10 affinityGroups
        var populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 10), TaskPopulateAndVerify.GenerationSpec.one(),
                Range.closedOpen(10, 20), TaskPopulateAndVerify.GenerationSpec.oneWithoutAffinity()
            )
        );
        //20/20(populationSpecs) = 1 tasks for each group
        var inDeletedTaskEntities = taskPopulateAndVerify.populate(0, 20, VirtualQueue.DELETED, populationSpecs);
        taskPopulateAndVerify.populate(0, 20, VirtualQueue.READY, populationSpecs);
        taskPopulateAndVerify.populate(0, 20, VirtualQueue.PARKED, populationSpecs);

        setFixedTime(2_000);

        //do
        var idVersionWithAffinityEntities = repository.readyToHardDelete(15);

        //verify
        var expectedIdVersionWithAffinity = inDeletedTaskEntities.stream()
            .sorted(Comparator.comparing(TaskEntity::getDeletedAt))
            .limit(15)
            .map(taskEntity -> idVersionMapper.map(taskEntity))
            .toList();
        assertThat(idVersionWithAffinityEntities).containsExactlyInAnyOrderElementsOf(expectedIdVersionWithAffinity);
    }

    @Test
    void shouldReadyToMoveFromParkedToReady() {
        //when
        setFixedTime(1_000);
        //total=10 affinityGroups
        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 10), TaskPopulateAndVerify.GenerationSpec.one(),
                // in case when tasks are created in task context by means of executionContext.withNewMessage
                Range.closedOpen(10, 12), TaskPopulateAndVerify.GenerationSpec.oneWithFixedWorkflow(now(clock)),
                // in case when several tasks are created not in task context via ExecutionContext.withAffinityGroup + executionContext.withNewMessage
                Range.closedOpen(12, 13), TaskPopulateAndVerify.GenerationSpec.oneWithFixedWorkflowAndDifferentTime()
            ) //total: 12 groups
        );

        taskPopulateAndVerify.populate(0, 2, VirtualQueue.READY, populationSpecs);
        taskPopulateAndVerify.populate(10, 11, VirtualQueue.READY, populationSpecs);
        //120/12(populationSpecs) = 10 tasks for each group
        var parkedTaskEntities = taskPopulateAndVerify.populate(0, 130, VirtualQueue.PARKED, populationSpecs);
        var inDeletedTaskEntities = taskPopulateAndVerify.populate(0, 14, VirtualQueue.DELETED, populationSpecs);

        var affinityGroupAndAffinityInDeleted = inDeletedTaskEntities.stream()
            .map(taskEntity -> new AffinityGroupAndAffinity(taskEntity.getAffinityGroup(), taskEntity.getAffinity()))
            .toList();

        setFixedTime(2_000);

        //do
        var idVersionEntities = repository.readyToMoveFromParkedToReady(affinityGroupAndAffinityInDeleted);

        //verify
        var movedShortTaskEntities = idVersionEntities.stream()
            .map(idVersion -> taskRepository.findById(idVersion.getId()).orElseThrow())
            .map(taskEntity -> taskEntity.toBuilder().virtualQueue(VirtualQueue.READY).build())
            .map(taskRepository::saveOrUpdate)
            .map(taskMapper::mapToShort)
            .toList();

        TaskPopulateAndVerify.VerifyVirtualQueueContext baseVerifyCtx = TaskPopulateAndVerify.VerifyVirtualQueueContext.builder()
            .populationSpecs(populationSpecs)
            .affectedTaskEntities(parkedTaskEntities)
            .movedShortTaskEntities(movedShortTaskEntities)
            .build();

        TaskPopulateAndVerify.VerifyVirtualQueueContext stillParkedVerifyCtx = baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(0, 2)) //range of groups
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 10), //rage on tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.untouched(VirtualQueue.PARKED)
            ))
            .build();
        taskPopulateAndVerify.verifyVirtualQueue(stillParkedVerifyCtx);

        stillParkedVerifyCtx = baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(10, 11)) //range of groups
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 10), //rage on tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.untouched(VirtualQueue.PARKED)
            ))
            .build();
        taskPopulateAndVerify.verifyVirtualQueue(stillParkedVerifyCtx);

        TaskPopulateAndVerify.VerifyVirtualQueueContext movedToReadyVerifyCtx = baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(3, 10)) //range of groups
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 1), //rage on tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.READY),

                Range.closedOpen(1, 10), //rage on tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.untouched(VirtualQueue.PARKED)
            ))
            .build();
        taskPopulateAndVerify.verifyVirtualQueue(movedToReadyVerifyCtx);

        TaskPopulateAndVerify.VerifyVirtualQueueContext movedWithSameWorkflowIdToReadyVerifyCtx = baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(11, 13)) //range of groups
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 10), //rage on tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.READY)
            ))
            .build();
        taskPopulateAndVerify.verifyVirtualQueue(movedWithSameWorkflowIdToReadyVerifyCtx);
    }

    @Test
    void shouldMoveParkedToReady() {
        //when
        setFixedTime(1_000);
        //total=10 affinityGroups
        var populationSpecs = taskPopulateAndVerify.makePopulationSpec(ImmutableMap.of(
                Range.closedOpen(0, 10), TaskPopulateAndVerify.GenerationSpec.one()
            )
        );
        var parkedTaskEntities = taskPopulateAndVerify.populate(0, 10, VirtualQueue.PARKED, populationSpecs);

        var shouldMovedIdVersions = parkedTaskEntities.stream()
            .map(idVersionMapper::mapToIdVersion);
        var concurrentMovedEntities = parkedTaskEntities.stream()
            .skip(5)
            .map(taskEntity -> taskEntity.toBuilder()
                .virtualQueue(VirtualQueue.DELETED)
                .build()
            )
            .toList();
        concurrentMovedEntities.forEach(taskRepository::saveOrUpdate);
        var shouldNotMovedIdVersions = concurrentMovedEntities.stream()
            .map(idVersionMapper::mapToIdVersion);
        var parkedToReadyRequest = Stream.concat(shouldMovedIdVersions, shouldNotMovedIdVersions).toList();

        setFixedTime(2_000);

        //do
        var movedShortTaskEntities = repository.moveParkedToReady(parkedToReadyRequest);

        //verify
        TaskPopulateAndVerify.VerifyVirtualQueueContext baseVerifyCtx = TaskPopulateAndVerify.VerifyVirtualQueueContext.builder()
            .populationSpecs(populationSpecs)
            .affectedTaskEntities(parkedTaskEntities)
            .movedShortTaskEntities(movedShortTaskEntities)
            .build();

        TaskPopulateAndVerify.VerifyVirtualQueueContext movedVerifyCtx = baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(0, 5)) //range of groups
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 1), //rage on tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.moved(VirtualQueue.READY)
            ))
            .build();
        taskPopulateAndVerify.verifyVirtualQueue(movedVerifyCtx);

        TaskPopulateAndVerify.VerifyVirtualQueueContext stillParkedVerifyCtx = baseVerifyCtx.toBuilder()
            .populationSpecRange(Range.closedOpen(5, 10)) //range of groups
            .expectedVirtualQueueByRange(Map.of(
                Range.closedOpen(0, 1), //rage on tasks in each group
                TaskPopulateAndVerify.ExpectedVirtualQueue.untouched(VirtualQueue.DELETED)
            ))
            .build();
        taskPopulateAndVerify.verifyVirtualQueue(stillParkedVerifyCtx);
    }

    private Set<AffinityGroupStat> toAffinityGroupStat(List<TaskPopulateAndVerify.PopulationSpec> affinityGroupAndAffinities, int limit) {
        return affinityGroupAndAffinities.stream()
            .map(populationSpec -> AffinityGroupStat.builder()
                .affinityGroup(populationSpec.getAffinityGroup())
                .number(limit)
                .build()
            )
            .collect(Collectors.toSet());
    }
}