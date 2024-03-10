package com.distributed_task_framework.perf_test.tasks;

import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.perf_test.persistence.repository.IntermediateResultRepository;
import com.distributed_task_framework.perf_test.persistence.repository.StressTestRunRepository;
import com.distributed_task_framework.perf_test.persistence.repository.StressTestSummaryRepository;

@Slf4j
public abstract class BasePerfTestTask<T> implements PerfTestTaskDefinitions<T> {
    @NonFinal
    @Autowired
    DistributedTaskService distributedTaskService;
    @NonFinal
    @Autowired
    StressTestRunRepository runRepository;
    @NonFinal
    @Autowired
    StressTestSummaryRepository summaryRepository;
    @NonFinal
    @Autowired
    IntermediateResultRepository intermediateResultRepository;

    @Override
    public boolean onFailureWithResult(FailedExecutionContext<T> failedExecutionContext) {
        log.error("onFailureWithResult(): failed task: payload={}, attempts={}, isLastAttempt={}",
                failedExecutionContext.getInputMessageOpt().orElse(null),
                failedExecutionContext.getFailures(),
                failedExecutionContext.isLastAttempt(),
                failedExecutionContext.getError()
        );
        return false;
    }
}
