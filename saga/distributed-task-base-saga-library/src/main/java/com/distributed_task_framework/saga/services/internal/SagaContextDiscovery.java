package com.distributed_task_framework.saga.services.internal;

import java.lang.annotation.Annotation;

public interface SagaContextDiscovery {

    <SAGA_METHOD extends Annotation> void registerMethod(String methodName, SAGA_METHOD sagaMethod);

    <SAGA_METHOD extends Annotation> void beginDetection(Class<SAGA_METHOD> sagaMethodClass);

    <SAGA_METHOD extends Annotation> SAGA_METHOD getSagaMethod();

    void completeDetection();
}
