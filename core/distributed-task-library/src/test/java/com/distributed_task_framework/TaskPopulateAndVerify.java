package com.distributed_task_framework;

import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.repository.TaskExtendedRepository;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import jakarta.annotation.Nullable;
import java.time.Clock;
import java.time.LocalDateTime;
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
        boolean withAffinityGroup;
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


        public static GenerationSpec allSetAndOneTask() {
            return GenerationSpec.of(
                    true,
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

        public static GenerationSpec noneSetAndOneTask() {
            return GenerationSpec.of(
                    false,
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

        public static GenerationSpec allSet(int taskNameNumber) {
            return GenerationSpec.of(
                    true,
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

        public static GenerationSpec canceled() {
            return GenerationSpec.of(
                    true,
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

        public static GenerationSpec allSetWithWorker(int taskNameNumber) {
            return GenerationSpec.of(
                    true,
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

        public static GenerationSpec allSetWithFixedWorker(int taskNameNumber, UUID fixedWorker) {
            return GenerationSpec.of(
                    true,
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

        public static GenerationSpec noneSet(int taskNameNumber) {
            return GenerationSpec.of(
                    false,
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

        public static GenerationSpec noneSetWitFixedTask(String taskName) {
            return GenerationSpec.of(
                    false,
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

        public static GenerationSpec noneSetWithFixedWorker(int taskNameNumber, UUID fixedWorker) {
            return GenerationSpec.of(
                    false,
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

        public static GenerationSpec oneTask(boolean withAffinityGroup, boolean withAffinity) {
            return GenerationSpec.of(
                    withAffinityGroup,
                    withAffinity,
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

        public static GenerationSpec withFixedWorker(boolean withAffinityGroup,
                                                     boolean withAffinity,
                                                     int taskNumber,
                                                     UUID fixedWorker) {
            return GenerationSpec.of(
                    withAffinityGroup,
                    withAffinity,
                    taskNumber,
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

        public static GenerationSpec noneSetAndOneTask(LocalDateTime fixedCreatedDate) {
            return GenerationSpec.of(
                    false,
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

        public static GenerationSpec withTasks(int taskNumber, LocalDateTime fixedCreatedDate) {
            return GenerationSpec.of(
                    false,
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

        public static GenerationSpec assigned(UUID fixedWorker, int taskNumber) {
            return GenerationSpec.of(
                    false,
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

        public static GenerationSpec noneSetAndOneTask(UUID fixedWorker, LocalDateTime fixedCreatedDate) {
            return GenerationSpec.of(
                    false,
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

        public static GenerationSpec withFixedAfgAndTaskName(String afg, String taskName, LocalDateTime fixedCreatedDate) {
            return GenerationSpec.of(
                    true,
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

        public static GenerationSpec withFixedAfgAndTaskName(String afg, String taskName) {
            return GenerationSpec.of(
                    true,
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
                                } else if (spec.withAffinityGroup) {
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
                                    LocalDateTime.now(clock).minusSeconds(size).plusSeconds(i);

                            return TaskEntity.builder()
                                    .taskName(populationSpec.getNameOfTasks().get(taskId))
                                    .virtualQueue(virtualQueue)
                                    .deletedAt(VirtualQueue.DELETED.equals(virtualQueue) ? LocalDateTime.now(clock) : null)
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

            var testedIds = ctx.getAffectedTaskEntities().stream()
                    .filter(taskEntity -> testedAffinityGroupAndAffinity.contains(
                                    Pair.of(taskEntity.getAffinityGroup(), taskEntity.getAffinity())
                            )
                    )
                    .skip((long) fromLimitInclude * (toPopulationSpecExclude - fromPopulationSpecInclude))
                    .limit((long) (toLimitExclude - fromLimitInclude) * (toPopulationSpecExclude - fromPopulationSpecInclude))
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
