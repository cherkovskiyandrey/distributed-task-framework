package com.distributed_task_framework.saga.test_service.services

import com.distributed_task_framework.saga.test_service.models.RemoteOneKotlinDto
import com.distributed_task_framework.saga.test_service.models.TestDataKotlinDto

interface TestKotlinService {
    fun run(testDataDto: TestDataKotlinDto): RemoteOneKotlinDto
}