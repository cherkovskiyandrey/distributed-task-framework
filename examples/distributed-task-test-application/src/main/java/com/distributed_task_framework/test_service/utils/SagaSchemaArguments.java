package com.distributed_task_framework.test_service.utils;

import com.google.common.collect.Lists;

import java.util.List;

public record SagaSchemaArguments(
        List<SagaArguments> orderedArguments
) {

    public static SagaSchemaArguments of(SagaArguments... orderedArguments) {
        return new SagaSchemaArguments(Lists.newArrayList(orderedArguments));
    }

}
