package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.TaskId;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public interface CompletionService {

    void waitCompletionAllWorkflow(UUID workflowId) throws TimeoutException, InterruptedException;

    void waitCompletionAllWorkflow(UUID workflowId, Duration timeout) throws TimeoutException, InterruptedException;

    void waitCompletion(TaskId taskId) throws TimeoutException, InterruptedException;

    void waitCompletion(TaskId taskId, Duration timeout) throws TimeoutException, InterruptedException;
}
