package com.distributed_task_framework.perf_test.tasks;

import com.distributed_task_framework.perf_test.persistence.entity.PerfTestState;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestIntermediateDto;
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
import com.distributed_task_framework.perf_test.model.Hierarchy;
import com.distributed_task_framework.perf_test.persistence.repository.StressTestRunRepository;
import com.distributed_task_framework.perf_test.persistence.repository.StressTestSummaryRepository;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestResultDto;

@Slf4j
@Component
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class PerfTestParentTask extends BasePerfTestTask<PerfTestIntermediateDto> {
    StressTestRunRepository runRepository;
    StressTestSummaryRepository summaryRepository;

    @Override
    public TaskDef<PerfTestIntermediateDto> getDef() {
        return STRESS_TEST_PARENT_TASK;
    }

    @Override
    public void execute(ExecutionContext<PerfTestIntermediateDto> executionContext) throws Exception {
        var intermediateDto = executionContext.getInputMessageOrThrow();
        var summaryId = intermediateDto.getSummaryId();
        var summary = summaryRepository.findById(summaryId).orElseThrow();
        var runId = intermediateDto.getRunId();
        var run = runRepository.findById(runId).orElseThrow();
        var parentHierarchy = Hierarchy.create(1);
        var joinList = Lists.<TaskId>newArrayList();
        var executionContextTemplate = ExecutionContext.withAffinityGroup(
                new Object(),
                run.getAffinityGroup(),
                summary.getAffinity()
        );

        for (int i = 0; i < run.getTotalTaskOnFirstLevel(); ++i) {
            TaskId taskId = distributedTaskService.schedule(
                    STRESS_TEST_CHILD_TASK_LEVEL_ONE,
                    executionContextTemplate.withNewMessage(intermediateDto.toBuilder()
                            .hierarchy(parentHierarchy.addLevel(i + 1))
                            .build()
                    )
            );
            joinList.add(taskId);
        }

        //schedule join
        distributedTaskService.scheduleJoin(
                STRESS_TEST_JOIN_TASK_LEVEL_ONE,
                executionContextTemplate.withNewMessage(PerfTestResultDto.builder()
                        .runId(runId)
                        .summaryId(summary.getId())
                        .build()
                ),
                joinList
        );

        summary = summary.toBuilder()
                .state(PerfTestState.IN_PROGRESS)
                .build();
        summaryRepository.save(summary);
    }
}
