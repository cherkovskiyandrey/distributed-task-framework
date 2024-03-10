package com.distributed_task_framework.perf_test.service.impl;

import com.distributed_task_framework.perf_test.model.PerfTestRunResult;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestRun;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestSummary;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.perf_test.mapper.PerfTestMapper;
import com.distributed_task_framework.perf_test.persistence.repository.StressTestRunRepository;
import com.distributed_task_framework.perf_test.persistence.repository.StressTestSummaryRepository;
import com.distributed_task_framework.perf_test.service.PerfTestService;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestGeneratedSpecDto;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.distributed_task_framework.perf_test.tasks.PerfTestTaskDefinitions.STRESS_TEST_GENERATED_TASK;

@Slf4j
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class PerfTestServiceImpl implements PerfTestService {
    StressTestRunRepository runRepository;
    StressTestSummaryRepository summaryRepository;
    DistributedTaskService distributedTaskService;
    PerfTestMapper perfTestMapper;

    @Transactional
    @Override
    public void run(PerfTestGeneratedSpecDto specDto) throws Exception {
        PerfTestRun testRun = perfTestMapper.toRun(specDto);
        runRepository.save(testRun);
        distributedTaskService.schedule(
                STRESS_TEST_GENERATED_TASK,
                ExecutionContext.simple(specDto)
        );
    }

    @Transactional(readOnly = true)
    @Override
    public PerfTestRunResult stat(String name) {
        var run = runRepository.findByName(name).orElseThrow();
        var summaries = summaryRepository.findAllByTestRunId(run.getId());
        var summaryStates = summaries.stream()
                .collect(Collectors.groupingBy(
                        PerfTestSummary::getState,
                        Collectors.counting()
                ));
        var completedAt = summaries.stream()
                .map(PerfTestSummary::getCompletedAt)
                .filter(Objects::nonNull)
                .max(LocalDateTime::compareTo)
                .orElse(null);

        return perfTestMapper.toTestResult(run, completedAt, summaryStates);
    }

    @Transactional
    @Override
    public void delete(String name) {
        runRepository.deleteByName(name);
    }
}
