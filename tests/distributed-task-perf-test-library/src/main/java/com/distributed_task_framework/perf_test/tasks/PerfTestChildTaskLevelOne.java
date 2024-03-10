package com.distributed_task_framework.perf_test.tasks;

import com.distributed_task_framework.perf_test.persistence.entity.PerfTestIntermediateResult;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestIntermediateDto;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestIntermediateResultDto;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestResultDto;

import java.util.List;

import static com.distributed_task_framework.perf_test.model.Hierarchy.JOIN_HIERARCHY_NAME;

@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.AT_LEAST_ONCE)
public class PerfTestChildTaskLevelOne extends BasePerfTestTask<PerfTestIntermediateDto> {

    @Override
    public TaskDef<PerfTestIntermediateDto> getDef() {
        return STRESS_TEST_CHILD_TASK_LEVEL_ONE;
    }

    @Override
    public void execute(ExecutionContext<PerfTestIntermediateDto> executionContext) throws Exception {
        var intermediateDto = executionContext.getInputMessageOrThrow();
        var run = runRepository.findById(intermediateDto.getRunId()).orElseThrow();
        var summaryId = intermediateDto.getSummaryId();
        var summary = summaryRepository.findById(summaryId).orElseThrow();
        var hierarchy = intermediateDto.getHierarchy();

        var intermediateResult = PerfTestIntermediateResult.builder()
                .affinityGroup(executionContext.getAffinityGroup())
                .affinity(executionContext.getAffinity())
                .hierarchy(hierarchy.toString())
                .number(summary.getNumber())
                .build();

        intermediateResult = intermediateResultRepository.saveOrUpdate(intermediateResult);
        //TimeUnit.MILLISECONDS.sleep(run.getTaskDurationMs());

        var messageToJoinTask = distributedTaskService.getJoinMessagesFromBranch(STRESS_TEST_JOIN_TASK_LEVEL_ONE)
                .get(0)
                .toBuilder()
                .message(PerfTestResultDto.builder()
                        .intermediateResultId(intermediateResult.getId())
                        .taskId(executionContext.getCurrentTaskId().getId())
                        .hierarchy(hierarchy)
                        .build()
                )
                .build();
        distributedTaskService.setJoinMessageToBranch(messageToJoinTask);

        List<TaskId> tasksToJoin = Lists.newArrayList();
        for (int i = 0; i < run.getTotalTaskOnSecondLevel(); ++i) {
            TaskId taskId = distributedTaskService.schedule(
                    STRESS_TEST_CHILD_TASK_LEVEL_TWO,
                    executionContext.withNewMessage(
                            intermediateDto.toBuilder()
                                    .parentIntermediateResultId(intermediateResult.getId())
                                    .hierarchy(intermediateDto.getHierarchy().addLevel(i + 1))
                                    .build()
                    )
            );
            tasksToJoin.add(taskId);
        }

        distributedTaskService.scheduleJoin(
                STRESS_TEST_JOIN_TASK_LEVEL_TWO,
                executionContext.withNewMessage(PerfTestIntermediateResultDto.builder()
                        .runId(run.getId())
                        .summaryId(summaryId)
                        .hierarchy(intermediateDto.getHierarchy().addLevel(JOIN_HIERARCHY_NAME))
                        .build()
                ),
                tasksToJoin
        );
    }
}
