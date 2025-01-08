package com.distributed_task_framework.saga.test_service.models

data class SagaRevertableKotlinDto<T>(
    var prevValue: T? = null,
    var newValue: T? = null
)
