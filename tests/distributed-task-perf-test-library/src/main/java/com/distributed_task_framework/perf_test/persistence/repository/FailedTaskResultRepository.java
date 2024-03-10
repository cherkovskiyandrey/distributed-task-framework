package com.distributed_task_framework.perf_test.persistence.repository;

import com.distributed_task_framework.perf_test.persistence.entity.PerfTestFailedTaskResult;
import org.springframework.data.repository.CrudRepository;

public interface FailedTaskResultRepository extends CrudRepository<PerfTestFailedTaskResult, Long> {
}
