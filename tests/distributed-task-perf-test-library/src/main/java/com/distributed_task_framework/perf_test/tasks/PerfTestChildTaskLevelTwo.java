package com.distributed_task_framework.perf_test.tasks;

import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestIntermediateDto;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestIntermediateResultDto;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestResultDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestIntermediateResult;

@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.AT_LEAST_ONCE)
public class PerfTestChildTaskLevelTwo extends BasePerfTestTask<PerfTestIntermediateDto> {

    @Override
    public TaskDef<PerfTestIntermediateDto> getDef() {
        return STRESS_TEST_CHILD_TASK_LEVEL_TWO;
    }

    @Override
    public void execute(ExecutionContext<PerfTestIntermediateDto> executionContext) throws Exception {
        var intermediateDto = executionContext.getInputMessageOrThrow();
        var parentIntermediateId = intermediateDto.getParentIntermediateResultId();
        var parentIntermediateDto = intermediateResultRepository.findById(parentIntermediateId).orElseThrow();
        var run = runRepository.findById(intermediateDto.getRunId()).orElseThrow();
        var hierarchy = intermediateDto.getHierarchy();

        var intermediateResult = PerfTestIntermediateResult.builder()
                .affinityGroup(executionContext.getAffinityGroup())
                .affinity(executionContext.getAffinity())
                .hierarchy(hierarchy.toString())
                .number(parentIntermediateDto.getNumber())
                .build();

        intermediateResult = intermediateResultRepository.saveOrUpdate(intermediateResult);
        //TimeUnit.MILLISECONDS.sleep(run.getTaskDurationMs());


        //add data to join task level two
        var messageToJoinLevelTwo = distributedTaskService.getJoinMessagesFromBranch(STRESS_TEST_JOIN_TASK_LEVEL_TWO)
                .get(0)
                .toBuilder()
                .message(PerfTestIntermediateResultDto.builder()
                        .intermediateResultId(intermediateResult.getId())
                        .taskId(executionContext.getCurrentTaskId().getId())
                        .build()
                )
                .build();
        distributedTaskService.setJoinMessageToBranch(messageToJoinLevelTwo);


        //add data to join task level one
        var messageToJoinLevelOne = distributedTaskService.getJoinMessagesFromBranch(STRESS_TEST_JOIN_TASK_LEVEL_ONE)
                .get(0)
                .toBuilder()
                .message(PerfTestResultDto.builder()
                        .intermediateResultId(intermediateResult.getId())
                        .taskId(executionContext.getCurrentTaskId().getId())
                        .hierarchy(hierarchy)
                        .build()
                )
                .build();
        distributedTaskService.setJoinMessageToBranch(messageToJoinLevelOne);
    }
}
