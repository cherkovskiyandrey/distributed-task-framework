package com.distributed_task_framework.test_service.services;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public interface SagaFlowWithoutResult {

    void waitCompletion() throws TimeoutException, InterruptedException;

    void waitCompletion(Duration duration) throws TimeoutException, InterruptedException;

    boolean isCompleted();
}
