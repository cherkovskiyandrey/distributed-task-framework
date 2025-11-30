package com.distributed_task_framework.saga.test_service.models

import jakarta.validation.constraints.NotEmpty
import org.springframework.validation.annotation.Validated
import java.time.Duration

@Validated
data class TestDataKotlinDto(
    var id: @NotEmpty Long,
    var version: @NotEmpty Long,

    var remoteServiceOneId: @NotEmpty String,
    var remoteOneData: @NotEmpty String,

    var remoteServiceTwoId: @NotEmpty String,
    var remoteTwoData: @NotEmpty String,

    var throwExceptionOnLevel: Int? = null,

    var levelToDelay: Int? = null,
    @io.swagger.v3.oas.annotations.media.Schema(type = "string", format = "duration")
    var syntheticDelayOnEachLevel: Duration? = null
)
