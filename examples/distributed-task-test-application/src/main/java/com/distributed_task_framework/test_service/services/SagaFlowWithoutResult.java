package com.distributed_task_framework.test_service.services;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public interface SagaFlowWithoutResult {

    void waitCompletion() throws TimeoutException;

    void waitCompletion(Duration duration) throws TimeoutException;

    boolean isCompleted();
}
