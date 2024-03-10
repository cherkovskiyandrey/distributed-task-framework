package com.distributed_task_framework.perf_test.persistence.repository;

import com.distributed_task_framework.perf_test.persistence.entity.PerfTestIntermediateResult;
import org.springframework.data.repository.CrudRepository;

public interface IntermediateResultRepository extends
        CrudRepository<PerfTestIntermediateResult, Long>,
        IntermediateResultExtendedRepository {
}
