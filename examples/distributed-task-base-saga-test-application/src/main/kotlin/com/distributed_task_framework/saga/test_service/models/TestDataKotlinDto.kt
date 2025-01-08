package com.distributed_task_framework.saga.test_service.models

import jakarta.validation.constraints.NotEmpty
import org.springframework.validation.annotation.Validated
import java.time.Duration

@Validated
data class TestDataKotlinDto(
    var id: @NotEmpty Long? = null,
    var version: @NotEmpty Long? = null,

    var remoteServiceOneId: @NotEmpty String? = null,
    var remoteOneData: @NotEmpty String? = null,

    var remoteServiceTwoId: @NotEmpty String? = null,
    var remoteTwoData: @NotEmpty String? = null,

    var throwExceptionOnLevel: Int? = null,

    var levelToDelay: Int? = null,
    @io.swagger.v3.oas.annotations.media.Schema(type = "string", format = "duration")
    var syntheticDelayOnEachLevel: Duration? = null
)
