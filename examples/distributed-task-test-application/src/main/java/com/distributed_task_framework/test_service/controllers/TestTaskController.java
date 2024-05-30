package com.distributed_task_framework.test_service.controllers;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.distributed_task_framework.test_service.tasks.dto.SimpleMessageDto;

@RestController
@RequestMapping("api/test-task")
public class TestTaskController {
    @Autowired
    private DistributedTaskService distributedTaskService;

    @Operation(summary = "Set test task")
    @PostMapping
    public void createTask(@RequestBody(required = false) SimpleMessageDto simpleMessageDto) throws Exception {
        distributedTaskService.schedule(PrivateTaskDefinitions.SIMPLE_CONSOLE_OUTPUT_TASK_DEF, ExecutionContext.simple(simpleMessageDto));
    }

    @Operation(summary = "Set root task for dynamic linked tasks")
    @PostMapping("parent-for-map-reduce-dag")
    public void createParentTask(@RequestBody(required = false) SimpleMessageDto simpleMessageDto) throws Exception {
        distributedTaskService.schedule(PrivateTaskDefinitions.PARENT_TASK, ExecutionContext.simple(simpleMessageDto));
    }

    @Operation(summary = "Check OOM", description = "don't forget to run app with -Xmx100m!!!!")
    @PostMapping("oom")
    public void createParentTask() throws Exception {
        distributedTaskService.schedule(PrivateTaskDefinitions.TEST_OOM_TASK, ExecutionContext.empty());
    }

    @Operation(summary = "Check task timeout")
    @PostMapping("check-timeout")
    public void createCheckTimeoutTask() throws Exception {
        distributedTaskService.schedule(PrivateTaskDefinitions.CHECK_TIMEOUT_TASK, ExecutionContext.empty());
    }
}
