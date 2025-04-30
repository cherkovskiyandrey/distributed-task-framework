package com.distributed_task_framework.saga.test_service.services.impl

import com.distributed_task_framework.saga.autoconfigure.annotations.SagaMethod
import com.distributed_task_framework.saga.services.DistributionSagaService
import com.distributed_task_framework.saga.test_service.models.RemoteOneKotlinDto
import com.distributed_task_framework.saga.test_service.models.SagaRevertableKotlinDto
import com.distributed_task_framework.saga.test_service.models.TestDataKotlinDto
import com.distributed_task_framework.saga.test_service.models.TestDataKotlinEntity
import com.distributed_task_framework.saga.test_service.services.TestKotlinService
import org.springframework.stereotype.Component


@Component
open class TestKotlinServiceImpl(
    private val distributionSagaService: DistributionSagaService
) : TestKotlinService {

    override fun run(testDataDto: TestDataKotlinDto): RemoteOneKotlinDto {
        return distributionSagaService.create("test-kotlin-saga")
            .registerToRun(this::createLocal, testDataDto)
            .thenRun(this::createOnRemoteServiceOne)
            .start()
            .get()
            .orElseThrow()
    }

    @SagaMethod(name = "createLocal-kotlin")
    fun createLocal(testDataDto: TestDataKotlinDto?): SagaRevertableKotlinDto<TestDataKotlinEntity> {
        return SagaRevertableKotlinDto(
            newValue = TestDataKotlinEntity(
                id = testDataDto?.id,
                version = testDataDto?.version,
                data = testDataDto?.remoteOneData
            )
        )
    }

    @SagaMethod(name = "createOnRemoteServiceOne-kotlin")
    fun createOnRemoteServiceOne(
        revertableTestDataEntity: SagaRevertableKotlinDto<TestDataKotlinEntity>,
        testDataDto: TestDataKotlinDto
    ): RemoteOneKotlinDto {
        return RemoteOneKotlinDto(
            remoteOneId = revertableTestDataEntity.newValue?.data,
            remoteOneData = testDataDto.remoteOneData
        )
    }
}