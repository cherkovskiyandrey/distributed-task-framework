package com.distributed_task_framework.saga.utils;

import com.distributed_task_framework.saga.exceptions.SagaArgumentResolvingException;
import lombok.Value;

import java.util.EnumMap;

@Value
public class ArgumentProvider {
    SagaSchemaArguments sagaSchemaArguments;
    EnumMap<SagaArguments, Object> argumentToObject;

    @SuppressWarnings("unchecked")
    public <T> T getById(int argId) {
        SagaArguments sagaArguments = sagaSchemaArguments.orderedArguments().get(argId);
        if (sagaArguments == null) {
            throw new SagaArgumentResolvingException("Unknown argId=[%d] for ArgumentProvider=[%s]".formatted(argId, this));
        }

        if (!argumentToObject.containsKey(sagaArguments)) {
            throw new SagaArgumentResolvingException("Unregistered argId=[%d] for ArgumentProvider=[%s]".formatted(argId, this));
        }
        return (T) argumentToObject.get(sagaArguments);
    }

    public int size() {
        return sagaSchemaArguments.orderedArguments().size();
    }
}
