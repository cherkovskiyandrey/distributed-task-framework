package com.distributed_task_framework.perf_test.persistence.repository;

import com.distributed_task_framework.perf_test.persistence.entity.PerfTestSummary;
import org.springframework.data.repository.CrudRepository;

import java.util.Collection;

public interface StressTestSummaryRepository extends CrudRepository<PerfTestSummary, Long> {

    Collection<PerfTestSummary> findAllByTestRunId(Long testRunId);

    int countAllByTestRunId(Long testRunId);
}
