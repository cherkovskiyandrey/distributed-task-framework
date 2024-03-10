package com.distributed_task_framework.perf_test.service;

import com.distributed_task_framework.perf_test.model.PerfTestRunResult;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestGeneratedSpecDto;

public interface PerfTestService {

    void run(PerfTestGeneratedSpecDto perfTestGeneratedSpecDto) throws Exception;

    PerfTestRunResult stat(String name);

    void delete(String name);
}
