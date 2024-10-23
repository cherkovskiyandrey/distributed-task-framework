package com.distributed_task_framework.perf_test.tasks;

import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestResultDto;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestErrorType;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestFailedTaskResult;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestIntermediateResult;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestRun;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestState;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestSummary;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.perf_test.model.Hierarchy;
import com.distributed_task_framework.perf_test.persistence.repository.FailedTaskResultRepository;
import com.distributed_task_framework.perf_test.persistence.repository.StressTestSummaryRepository;

import jakarta.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.distributed_task_framework.perf_test.model.Hierarchy.JOIN_HIERARCHY_NAME;

@Slf4j
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
@RequiredArgsConstructor
public class PerfTestJoinTaskLevelOne extends BasePerfTestTask<PerfTestResultDto> {
    StressTestSummaryRepository stressTestSummaryRepository;
    FailedTaskResultRepository failedTaskResultRepository;

    @Override
    public TaskDef<PerfTestResultDto> getDef() {
        return STRESS_TEST_JOIN_TASK_LEVEL_ONE;
    }

    @Override
    public void execute(ExecutionContext<PerfTestResultDto> executionContext) throws Exception {
        var stressTestResultDto = executionContext.getInputMessageOrThrow();
        var inputJoinTaskMessages = executionContext.getInputJoinTaskMessages();

        var runId = stressTestResultDto.getRunId();
        var run = runRepository.findById(runId).orElseThrow();
        var summaryId = stressTestResultDto.getSummaryId();
        var summary = stressTestSummaryRepository.findById(summaryId).orElseThrow();
        var expectedTaskNumberFirstLevel = run.getTotalTaskOnFirstLevel();
        var expectedTaskNumberSecondLevel = run.getTotalTaskOnSecondLevel();
        var number = summary.getNumber();

        var resultByHierarchy = inputJoinTaskMessages.stream()
                .collect(Collectors.toMap(
                        PerfTestResultDto::getHierarchy,
                        Function.identity(),
                        (l, r) -> r
                ));

        boolean hasFailed = false;
        for (int i = 0; i < expectedTaskNumberFirstLevel; ++i) {
            var expectedHierarchyLevelOne = Hierarchy.create(1).addLevel(i + 1);
            var realResultLevelOne = resultByHierarchy.get(expectedHierarchyLevelOne);
            hasFailed |= handleLevel(
                    expectedHierarchyLevelOne,
                    number,
                    run,
                    summary,
                    realResultLevelOne
            );

            for (int j = 0; j < expectedTaskNumberSecondLevel; ++j) {
                var expectedHierarchyLevelTwo = expectedHierarchyLevelOne.addLevel(j + 1);
                var realResultLevelTwo = resultByHierarchy.get(expectedHierarchyLevelTwo);
                hasFailed |= handleLevel(
                        expectedHierarchyLevelTwo,
                        number,
                        run,
                        summary,
                        realResultLevelTwo
                );
            }

            var expectedHierarchyJoinLevelTwo = expectedHierarchyLevelOne.addLevel(JOIN_HIERARCHY_NAME);
            var realResulJointLevelTwo = resultByHierarchy.get(expectedHierarchyJoinLevelTwo);
            var expectedNumber = expectedTaskNumberSecondLevel * number;
            hasFailed |= handleLevel(
                    expectedHierarchyJoinLevelTwo,
                    expectedNumber,
                    run,
                    summary,
                    realResulJointLevelTwo
            );
        }
        summary = summary.toBuilder()
                .state(hasFailed ? PerfTestState.FAILED : PerfTestState.DONE)
                .completedAt(LocalDateTime.now())
                .build();
        stressTestSummaryRepository.save(summary);
    }

    private boolean handleLevel(Hierarchy expectedHierarchy,
                                long expectedNumber,
                                PerfTestRun run,
                                PerfTestSummary summary,
                                @Nullable PerfTestResultDto realResult) {
        if (realResult == null) {
            log.error(
                    "handleLevel(): not found input message for expectedHierarchy=[{}], run=[{}], summary=[{}]",
                    expectedHierarchy,
                    run,
                    summary
            );
            reportEmptyResult(run, summary, expectedHierarchy, PerfTestErrorType.INPUT_MESSAGE_NOT_FOUND);
            return true;
        }

        var intermediateResultId = realResult.getIntermediateResultId();
        var realIntermediateResultOpt = intermediateResultRepository.findById(intermediateResultId);
        if (realIntermediateResultOpt.isEmpty()) {
            log.error(
                    "execute(): not found intermediate result for expectedHierarchy=[{}], run=[{}], summary=[{}]",
                    expectedHierarchy,
                    run,
                    summary
            );
            reportEmptyResult(run, summary, expectedHierarchy, PerfTestErrorType.INTERMEDIATE_RESULT_NOT_FOUND);
            return true;
        }

        var expectedIntermediateResult = PerfTestIntermediateResult.builder()
                .affinityGroup(run.getAffinityGroup())
                .affinity(summary.getAffinity())
                .hierarchy(expectedHierarchy.toString())
                .number(expectedNumber)
                .build();
        var realIntermediateResult = realIntermediateResultOpt.get();
        if (!Objects.equals(expectedIntermediateResult, realIntermediateResult)) {
            log.error(
                    "execute(): unexpected intermediate result expectedIntermediateResult=[{}], " +
                            "realIntermediateResult=[{}], run=[{}], summary=[{}]",
                    expectedIntermediateResult,
                    realIntermediateResult,
                    run,
                    summary
            );
            reportError(
                    run,
                    summary,
                    expectedHierarchy,
                    PerfTestErrorType.UNEXPECTED_INTERMEDIATE_RESULT,
                    realResult.getTaskId(),
                    realIntermediateResult
            );
            return true;
        }

        return false;
    }

    private void reportEmptyResult(PerfTestRun run,
                                   PerfTestSummary perfTestSummary,
                                   Hierarchy expectedHierarchy,
                                   PerfTestErrorType perfTestErrorType) {
        reportError(run, perfTestSummary, expectedHierarchy, perfTestErrorType, null, null);
    }

    private void reportError(PerfTestRun run,
                             PerfTestSummary perfTestSummary,
                             Hierarchy expectedHierarchy,
                             PerfTestErrorType perfTestErrorType,
                             @Nullable UUID taskId,
                             @Nullable PerfTestIntermediateResult perfTestIntermediateResult) {
        var failedEmptyResult = PerfTestFailedTaskResult.builder()
                .runId(run.getId())
                .summaryId(perfTestSummary.getId())
                .perfTestErrorType(perfTestErrorType)
                .hierarchy(expectedHierarchy.toString())
                .taskId(taskId)
                .build();

        if (perfTestIntermediateResult != null) {
            failedEmptyResult = failedEmptyResult.toBuilder()
                    .realNumber(perfTestIntermediateResult.getNumber())
                    .build();
        }
        failedTaskResultRepository.save(failedEmptyResult);
    }

    @Override
    public boolean onFailureWithResult(FailedExecutionContext<PerfTestResultDto> failedExecutionContext) {
        boolean result = super.onFailureWithResult(failedExecutionContext);
        var inputMessageOpt = failedExecutionContext.getInputMessageOpt();
        if (inputMessageOpt.isEmpty()) {
            log.error(
                    "onFailureWithResult(): empty input message failedExecutionContext=[{}]",
                    failedExecutionContext
            );
            return result;
        }
        var inputMessage = inputMessageOpt.get();
        var summaryId = inputMessage.getSummaryId();
        if (summaryId == null) {
            log.error(
                    "onFailureWithResult(): empty summaryId failedExecutionContext=[{}]",
                    failedExecutionContext
            );
            return result;
        }
        var summaryOpt = stressTestSummaryRepository.findById(summaryId);
        if (summaryOpt.isEmpty()) {
            log.error(
                    "onFailureWithResult(): summary couldn't find in db failedExecutionContext=[{}]",
                    failedExecutionContext
            );
            return result;
        }
        var summary = summaryOpt.get();
        summary = summary.toBuilder()
                .state(PerfTestState.FAILED)
                .build();
        stressTestSummaryRepository.save(summary);
        return result;
    }
}
