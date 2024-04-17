package com.distributed_task_framework.test_service.utils;

import java.util.EnumMap;

public class ArgumentProviderBuilder {
    private final SagaSchemaArguments sagaSchemaArguments;
    private final EnumMap<SagaArguments, Object> argumentToObject = new EnumMap<>(SagaArguments.class);

    public ArgumentProviderBuilder(SagaSchemaArguments sagaSchemaArguments) {
        this.sagaSchemaArguments = sagaSchemaArguments;
    }

    public <T> void reg(SagaArguments sagaArgument, T arg) {
        argumentToObject.put(sagaArgument, arg);
    }

    public ArgumentProvider build() {
        return new ArgumentProvider(sagaSchemaArguments, argumentToObject);
    }
}
