package com.distributed_task_framework;

import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.TaskExtendedRepository;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@FieldDefaults(level = AccessLevel.PRIVATE)
public class TaskPopulateAndVerify {
    public static final String TASK_PREFIX = "task_";
    public static final String WORKER_PREFIX = "worker-number-";

    @Autowired
    @Qualifier("taskExtendedRepositoryImpl")
    TaskExtendedRepository taskExtendedRepository;
    @Autowired
    Clock clock;

    @Value
    @Builder
    public static class PopulationSpec {
        @Nullable
        String affinityGroup;
        @Nullable
        String affinity;
        @Builder.Default
        List<String> nameOfTasks = List.of("test_task");
        @Nullable
        UUID assignedWorker;
        @Nullable
        LocalDateTime fixedCreatedDate;
        @Nullable
        String fixedAffinityGroup;
        @Nullable
        String fixedAffinity;
        @Nullable
        UUID fixedWorkflowId;
        @Nullable
        String fixedTaskName;
        boolean deferred;
        boolean canceled;
    }

    @Value(staticConstructor = "of")
    public static class GenerationSpec {
        boolean withAffinity;
        int taskNameNumber;
        boolean withWorker;
        boolean deferred;
        boolean canceled;
        @Nullable
        UUID fixedWorker;
        @Nullable
        LocalDateTime fixedCreatedDate;
        @Nullable
        String fixedAffinityGroup;
        @Nullable
        String fixedAffinity;
        @Nullable
        String fixedWorkflowId;
        @Nullable
        String fixedTaskName;


        public static GenerationSpec one() {
            return GenerationSpec.of(
                true,
                1,
                false,
                false,
                false,
                null,
                null,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec oneWithoutAffinity() {
            return GenerationSpec.of(
                false,
                1,
                false,
                false,
                false,
                null,
                null,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec of(int taskNameNumber) {
            return GenerationSpec.of(
                true,
                taskNameNumber,
                false,
                false,
                false,
                null,
                null,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec withoutAffinity(int taskNameNumber) {
            return GenerationSpec.of(
                false,
                taskNameNumber,
                false,
                false,
                false,
                null,
                null,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec oneCanceled() {
            return GenerationSpec.of(
                true,
                1,
                false,
                false,
                true,
                null,
                null,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec deferred(int taskNameNumber) {
            return GenerationSpec.of(
                true,
                taskNameNumber,
                false,
                true,
                false,
                null,
                null,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec withAutoAssignedWorker(int taskNameNumber) {
            return GenerationSpec.of(
                true,
                taskNameNumber,
                true,
                false,
                false,
                null,
                null,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec withWorker(int taskNameNumber, UUID fixedWorker) {
            return GenerationSpec.of(
                true,
                taskNameNumber,
                true,
                false,
                false,
                fixedWorker,
                null,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec withWorkerAndWithoutAffinity(int taskNameNumber, UUID fixedWorker) {
            return GenerationSpec.of(
                false,
                taskNameNumber,
                true,
                false,
                false,
                fixedWorker,
                null,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec oneWithTaskNameAndWithoutAffinity(String taskName) {
            return GenerationSpec.of(
                false,
                1,
                false,
                false,
                false,
                null,
                null,
                null,
                null,
                null,
                taskName
            );
        }

        public static GenerationSpec oneWithCreatedDateAndWithoutAffinity(LocalDateTime fixedCreatedDate) {
            return GenerationSpec.of(
                false,
                1,
                false,
                false,
                false,
                null,
                fixedCreatedDate,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec withCreatedDateAndWithoutAffinity(int taskNumber,
                                                                       LocalDateTime fixedCreatedDate) {
            return GenerationSpec.of(
                false,
                taskNumber,
                false,
                false,
                false,
                null,
                fixedCreatedDate,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec withWorkerAndWithoutAffinity(UUID fixedWorker, int taskNumber) {
            return GenerationSpec.of(
                false,
                taskNumber,
                false,
                false,
                false,
                fixedWorker,
                null,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec oneWithWorkerAndCreatedDateAndWithoutAffinity(UUID fixedWorker,
                                                                                   LocalDateTime fixedCreatedDate) {
            return GenerationSpec.of(
                false,
                1,
                false,
                false,
                false,
                fixedWorker,
                fixedCreatedDate,
                null,
                null,
                null,
                null
            );
        }

        public static GenerationSpec oneWithAffinityGroupAndTaskNameAndCreatedDate(String afg,
                                                                                   String taskName,
                                                                                   LocalDateTime fixedCreatedDate) {
            return GenerationSpec.of(
                true,
                1,
                false,
                false,
                false,
                null,
                fixedCreatedDate,
                afg,
                null,
                null,
                taskName
            );
        }

        public static GenerationSpec oneWithAffinityGroupAndTaskName(String afg, String taskName) {
            return GenerationSpec.of(
                true,
                1,
                false,
                false,
                false,
                null,
                null,
                afg,
                null,
                null,
                taskName
            );
        }
    }

    public static String getTaskName(int id) {
        return TASK_PREFIX + id;
    }

    public static UUID getNode(int i) {
        return UUID.nameUUIDFromBytes((WORKER_PREFIX + i).getBytes());
    }

    public static String getAffinityGroup(int id) {
        return "" + id;
    }

    public List<PopulationSpec> makePopulationSpec(ImmutableMap<Range<Integer>, GenerationSpec> generateSpecs) {
        List<PopulationSpec> result = Lists.newArrayList();
        for (var entity : generateSpecs.entrySet()) {
            int fromInclude = entity.getKey().lowerEndpoint();
            int toExclude = entity.getKey().upperEndpoint();
            GenerationSpec spec = entity.getValue();

            List<PopulationSpec> generationList = IntStream.range(fromInclude, toExclude)
                .mapToObj(i -> {
                        final UUID assignedWorker;
                        if (spec.fixedWorker != null) {
                            assignedWorker = spec.fixedWorker;
                        } else if (spec.withWorker) {
                            assignedWorker = getNode(i);
                        } else {
                            assignedWorker = null;
                        }

                        final String affinityGroup;
                        if (spec.getFixedAffinityGroup() != null) {
                            affinityGroup = spec.getFixedAffinityGroup();
                        } else if (spec.withAffinity) {
                            affinityGroup = getAffinityGroup(i);
                        } else {
                            affinityGroup = null;
                        }

                        final String affinity;
                        if (spec.getFixedAffinity() != null) {
                            affinity = spec.getFixedAffinity();
                        } else if (spec.withAffinity) {
                            affinity = "" + i;
                        } else {
                            affinity = null;
                        }

                        List<String> taskNames = Lists.newArrayList();
                        if (spec.getFixedTaskName() != null) {
                            taskNames.add(spec.fixedTaskName);
                        } else {
                            taskNames = IntStream.range(0, spec.getTaskNameNumber())
                                .mapToObj(TaskPopulateAndVerify::getTaskName)
                                .toList();
                        }

                        UUID workflowId = spec.getFixedWorkflowId() != null ?
                            UUID.nameUUIDFromBytes(spec.getFixedWorkflowId().getBytes()) :
                            null;
                        return PopulationSpec.builder()
                            .affinityGroup(affinityGroup)
                            .affinity(affinity)
                            .nameOfTasks(taskNames)
                            .assignedWorker(assignedWorker)
                            .fixedCreatedDate(spec.getFixedCreatedDate())
                            .fixedWorkflowId(workflowId)
                            .deferred(spec.isDeferred())
                            .canceled(spec.isCanceled())
                            .build();
                    }
                )
                .toList();
            result.addAll(generationList);
        }
        return result;
    }

    public Collection<TaskEntity> populate(int fromInclude,
                                           int toExclude,
                                           VirtualQueue virtualQueue,
                                           List<PopulationSpec> populationSpecs) {
        Map<PopulationSpec, Integer> taskIndexMap = Maps.newHashMap();
        int size = toExclude - fromInclude;
        List<TaskEntity> taskEntities = IntStream.range(fromInclude, toExclude)
            .mapToObj(i -> {
                    int groupIdx = i % populationSpecs.size();
                    var populationSpec = populationSpecs.get(groupIdx);
                    var taskId = taskIndexMap.compute(
                        populationSpec,
                        (k, prev) -> prev == null ? 0 : (prev + 1) % k.getNameOfTasks().size()
                    );
                    LocalDateTime createdDateTime = populationSpec.getFixedCreatedDate() != null ?
                        populationSpec.getFixedCreatedDate() :
                        now().minusSeconds(size).plusSeconds(i);

                    return TaskEntity.builder()
                        .taskName(populationSpec.getNameOfTasks().get(taskId))
                        .virtualQueue(virtualQueue)
                        .deletedAt(VirtualQueue.DELETED.equals(virtualQueue) ? now() : null)
                        .affinityGroup(populationSpec.getAffinityGroup())
                        .affinity(populationSpec.getAffinity())
                        .workflowId(populationSpec.fixedWorkflowId != null ? populationSpec.fixedWorkflowId : UUID.randomUUID())
                        .assignedWorker(populationSpec.getAssignedWorker())
                        .createdDateUtc(createdDateTime)
                        .workflowCreatedDateUtc(createdDateTime)
                        .executionDateUtc(createdDateTime)
                        .notToPlan(populationSpec.isDeferred())
                        .canceled(populationSpec.isCanceled())
                        .build();
                }
            )
            .toList();
        return taskExtendedRepository.saveAll(taskEntities);
    }

    private static LocalDateTime truncateLocalDateTime(LocalDateTime time) {
        if (time == null)
            return null;

        return time.truncatedTo(ChronoUnit.MICROS);
    }

    private LocalDateTime now() {
        // On Linux it has nanoseconds, which after rounding in DB leads to non-equal dates
        // https://stackoverflow.com/questions/65594874/java-15-nanoseconds-precision-causing-issues-on-linux-environment
        return LocalDateTime.now(clock)
            .truncatedTo(ChronoUnit.MICROS);
    }

    public Set<UUID> knownNodes(List<TaskPopulateAndVerify.PopulationSpec> knownPopulationSpecs) {
        return knownPopulationSpecs.stream()
            .map(TaskPopulateAndVerify.PopulationSpec::getAssignedWorker)
            .collect(Collectors.toSet());
    }

    @Value
    @Builder(toBuilder = true)
    public static class VerifyVirtualQueueContext {
        Range<Integer> populationSpecRange;
        Map<Range<Integer>, TaskPopulateAndVerify.ExpectedVirtualQueue> expectedVirtualQueueByRange;
        List<TaskPopulateAndVerify.PopulationSpec> populationSpecs;
        Collection<TaskEntity> affectedTaskEntities;
        List<ShortTaskEntity> movedShortTaskEntities;

    }

    public Map<VirtualQueue, List<TaskEntity>> verifyVirtualQueue(VerifyVirtualQueueContext ctx) {
        Map<VirtualQueue, List<TaskEntity>> groupedTasks = Maps.newHashMap();
        int fromPopulationSpecInclude = ctx.getPopulationSpecRange().lowerEndpoint();
        int toPopulationSpecExclude = ctx.getPopulationSpecRange().upperEndpoint();
        Set<Pair<String, String>> testedAffinityGroupAndAffinity =
            ctx.getPopulationSpecs().subList(fromPopulationSpecInclude, toPopulationSpecExclude).stream()
                .map(spec -> Pair.of(spec.getAffinityGroup(), spec.getAffinity()))
                .collect(Collectors.toSet());

        for (var entity : ctx.getExpectedVirtualQueueByRange().entrySet()) {
            int fromLimitInclude = entity.getKey().lowerEndpoint();
            int toLimitExclude = entity.getKey().upperEndpoint();
            ExpectedVirtualQueue expectedVirtualQueue1 = entity.getValue();
            VirtualQueue expectedVirtualQueue = expectedVirtualQueue1.getVirtualQueue();
            Boolean wereMoved = expectedVirtualQueue1.getWereMoved();
            int groups = toPopulationSpecExclude - fromPopulationSpecInclude;

            var testedIds = ctx.getAffectedTaskEntities().stream()
                .filter(taskEntity -> testedAffinityGroupAndAffinity.contains(
                        Pair.of(taskEntity.getAffinityGroup(), taskEntity.getAffinity())
                    )
                )
                .skip((long) fromLimitInclude * groups)
                .limit((long) (toLimitExclude - fromLimitInclude) * groups)
                .map(TaskEntity::getId)
                .collect(Collectors.toSet());
            var testedTasks = taskExtendedRepository.findAll(testedIds);
            Assertions.assertThat(testedTasks)
                .map(TaskEntity::getVirtualQueue)
                .allMatch(expectedVirtualQueue::equals);
            groupedTasks.put(expectedVirtualQueue, testedTasks);

            if (wereMoved == null) {
                continue;
            }

            Map<UUID, VirtualQueue> movedIdToVq = ctx.getMovedShortTaskEntities().stream()
                .collect(Collectors.toMap(
                    ShortTaskEntity::getId,
                    ShortTaskEntity::getVirtualQueue
                ));
            if (wereMoved) {
                assertThat(movedIdToVq.keySet()).containsAll(testedIds);
                Set<VirtualQueue> movedParked = testedIds.stream()
                    .map(movedIdToVq::get)
                    .collect(Collectors.toSet());
                assertThat(movedParked).singleElement().isEqualTo(expectedVirtualQueue);
            } else {
                assertThat(movedIdToVq.keySet()).doesNotContainAnyElementsOf(testedIds);
            }
        }
        return groupedTasks;
    }

    @Value(staticConstructor = "of")
    public static class ExpectedVirtualQueue {
        VirtualQueue virtualQueue;
        Boolean wereMoved;

        public static ExpectedVirtualQueue in(VirtualQueue virtualQueue) {
            return of(virtualQueue, null);
        }

        public static ExpectedVirtualQueue moved(VirtualQueue virtualQueue) {
            return of(virtualQueue, true);
        }

        public static ExpectedVirtualQueue untouched(VirtualQueue virtualQueue) {
            return of(virtualQueue, false);
        }
    }
}
