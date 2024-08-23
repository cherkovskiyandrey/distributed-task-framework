package com.distributed_task_framework.perf_test.tasks;

import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestGeneratedSpecDto;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestIntermediateDto;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestRun;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestState;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestSummary;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.perf_test.persistence.repository.StressTestRunRepository;
import com.distributed_task_framework.perf_test.persistence.repository.StressTestSummaryRepository;

@Slf4j
@Component
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.AT_LEAST_ONCE)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class PerfTestGeneratedTask extends BasePerfTestTask<PerfTestGeneratedSpecDto> {
    StressTestRunRepository stressTestRunRepository;
    StressTestSummaryRepository stressTestSummaryRepository;

    @Override
    public TaskDef<PerfTestGeneratedSpecDto> getDef() {
        return STRESS_TEST_GENERATED_TASK;
    }

    @Override
    public void execute(ExecutionContext<PerfTestGeneratedSpecDto> executionContext) throws Exception {
        var spec = executionContext.getInputMessageOrThrow();
        var testRun = stressTestRunRepository.findByName(spec.getName()).orElseThrow();

        for (int i = 0; i < spec.getTotalPipelines(); ++i) {
            var currentAffinity = i % spec.getTotalAffinities();
            var summary = createSummary(testRun, currentAffinity, i);
            distributedTaskService.schedule(
                    STRESS_TEST_PARENT_TASK,
                    ExecutionContext.simple(
                            PerfTestIntermediateDto.builder()
                                    .runId(testRun.getId())
                                    .summaryId(summary.getId())
                                    .build()
                    )
            );
        }
    }

    private PerfTestSummary createSummary(PerfTestRun perfTestRun,
                                          int currentAffinity,
                                          long testId) {
        var summary = PerfTestSummary.builder()
                .testRunId(perfTestRun.getId())
                .testId(testId)
                .affinity("" + currentAffinity)
                .number((long) RandomUtils.nextInt(1, Integer.MAX_VALUE))
                .state(PerfTestState.NEW)
                .build();
        return stressTestSummaryRepository.save(summary);
    }
}
