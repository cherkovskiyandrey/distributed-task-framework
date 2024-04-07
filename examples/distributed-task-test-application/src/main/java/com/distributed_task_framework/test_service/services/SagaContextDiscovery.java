package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.test_service.annotations.SagaMethod;

public interface SagaContextDiscovery {

    void registerMethod(String methodName, SagaMethod sagaMethodAnnotation);

    void beginDetection();

    SagaMethod getSagaMethod();

    void completeDetection();
}
