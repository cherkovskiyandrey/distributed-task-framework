package com.distributed_task_framework.perf_test.controller;

import com.distributed_task_framework.perf_test.model.PerfTestRunResult;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.distributed_task_framework.perf_test.service.PerfTestService;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestGeneratedSpecDto;

import jakarta.validation.Valid;

@RestController
@RequestMapping("api/dtf/perf-test")
public class PerfTestController {
    @Autowired
    PerfTestService perfTestService;

    @Operation(summary = "Run stress test")
    @PostMapping
    public void run(@RequestBody(required = false) @Valid PerfTestGeneratedSpecDto specDto) throws Exception {
        perfTestService.run(specDto);
    }

    @Operation(summary = "Retrieve current result by test name")
    @GetMapping("/{name}")
    public PerfTestRunResult result(@PathVariable(name = "name") String name) {
        return perfTestService.stat(name);
    }

    @Operation(summary = "Delete current result by test name")
    @DeleteMapping("/{name}")
    public void delete(@PathVariable(name = "name") String name) {
        perfTestService.delete(name);
    }
}
