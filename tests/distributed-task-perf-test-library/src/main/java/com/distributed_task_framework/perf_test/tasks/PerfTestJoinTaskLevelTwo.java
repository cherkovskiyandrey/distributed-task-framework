package com.distributed_task_framework.perf_test.tasks;

import com.distributed_task_framework.perf_test.persistence.entity.PerfTestIntermediateResult;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestIntermediateResultDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestResultDto;

import java.util.Optional;


@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
@RequiredArgsConstructor
public class PerfTestJoinTaskLevelTwo extends BasePerfTestTask<PerfTestIntermediateResultDto> {

    @Override
    public TaskDef<PerfTestIntermediateResultDto> getDef() {
        return STRESS_TEST_JOIN_TASK_LEVEL_TWO;
    }

    @Override
    public void execute(ExecutionContext<PerfTestIntermediateResultDto> executionContext) throws Exception {
        var intermediateResultDto = executionContext.getInputMessageOrThrow();
        var hierarchy = intermediateResultDto.getHierarchy();
        var inputJoinTaskMessages = executionContext.getInputJoinTaskMessages();
        var runId = intermediateResultDto.getRunId();
        var run = runRepository.findById(runId).orElseThrow();

        if (inputJoinTaskMessages.size() != run.getTotalTaskOnSecondLevel()) {
            log.error(
                    "execute(): mismatch inputJoinTaskMessages=[{}] != taskNumberOnSecondLevel=[{}]",
                    inputJoinTaskMessages.size(),
                    run.getTotalTaskOnSecondLevel()
            );
        }

        long sum = inputJoinTaskMessages.stream()
                .map(PerfTestIntermediateResultDto::getIntermediateResultId)
                .map(intermediateResultRepository::findById)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .mapToLong(PerfTestIntermediateResult::getNumber)
                .sum();

        var intermediateResult = PerfTestIntermediateResult.builder()
                .affinityGroup(executionContext.getAffinityGroup())
                .affinity(executionContext.getAffinity())
                .hierarchy(hierarchy.toString())
                .number(sum)
                .build();

        intermediateResult = intermediateResultRepository.saveOrUpdate(intermediateResult);

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
