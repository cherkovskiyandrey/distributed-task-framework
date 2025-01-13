package com.distributed_task_framework.saga.generator;

import com.distributed_task_framework.saga.settings.SagaSettings;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TestSagaModel<T> {
    String name;
    T bean;
    SagaSettings sagaSettings;
}
