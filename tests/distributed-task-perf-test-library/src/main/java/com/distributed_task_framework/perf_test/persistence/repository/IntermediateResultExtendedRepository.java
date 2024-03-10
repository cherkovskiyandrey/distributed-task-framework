package com.distributed_task_framework.perf_test.persistence.repository;

import com.distributed_task_framework.perf_test.persistence.entity.PerfTestIntermediateResult;

public interface IntermediateResultExtendedRepository {

    PerfTestIntermediateResult saveOrUpdate(PerfTestIntermediateResult perfTestIntermediateResult);
}
