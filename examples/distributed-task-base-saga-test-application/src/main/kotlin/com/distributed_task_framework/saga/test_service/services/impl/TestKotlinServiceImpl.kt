package com.distributed_task_framework.saga.test_service.services.impl

import com.distributed_task_framework.saga.annotations.SagaMethod
import com.distributed_task_framework.saga.services.DistributionSagaService
import com.distributed_task_framework.saga.test_service.models.RemoteOneKotlinDto
import com.distributed_task_framework.saga.test_service.models.SagaRevertableKotlinDto
import com.distributed_task_framework.saga.test_service.models.TestDataKotlinDto
import com.distributed_task_framework.saga.test_service.models.TestDataKotlinEntity
import com.distributed_task_framework.saga.test_service.services.TestKotlinService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Lazy
import org.springframework.stereotype.Component


@Component
open class TestKotlinServiceImpl(
    private val distributionSagaService: DistributionSagaService
) : TestKotlinService {

    @Lazy
    @Autowired
    private lateinit var self: TestKotlinServiceImpl

    override fun run(testDataDto: TestDataKotlinDto): RemoteOneKotlinDto {
        return distributionSagaService.create("test-kotlin-saga")
            .registerToRun(
                self::createLocal,
                testDataDto
            )
            .thenRun(
                self::createOnRemoteServiceOne
            )
            .start()
            .get()
            .orElseThrow()
    }

    //todo: support private accessor in kotlin and in java ?
    @SagaMethod(name = "createLocal-kotlin")
    open fun createLocal(testDataDto: TestDataKotlinDto?): SagaRevertableKotlinDto<TestDataKotlinEntity> {
        return SagaRevertableKotlinDto(
            newValue = TestDataKotlinEntity(
                id = testDataDto?.id,
                version = testDataDto?.version,
                data = testDataDto?.remoteOneData
            )
        )
    }

    @SagaMethod(name = "createOnRemoteServiceOne-kotlin")
    open fun createOnRemoteServiceOne(
        revertableTestDataEntity: SagaRevertableKotlinDto<TestDataKotlinEntity>,
        testDataDto: TestDataKotlinDto
    ): RemoteOneKotlinDto {
        return RemoteOneKotlinDto(
            remoteOneId = revertableTestDataEntity.newValue?.data,
            remoteOneData = testDataDto.remoteOneData
        )
    }
}