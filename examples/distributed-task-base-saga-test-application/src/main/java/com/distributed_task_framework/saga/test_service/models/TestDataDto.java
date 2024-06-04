package com.distributed_task_framework.saga.test_service.models;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.NotEmpty;

@Validated
@Value
@Builder
@Jacksonized
public class TestDataDto {
    @NotEmpty
    Long id;
    @NotEmpty
    Long version;

    @NotEmpty
    String remoteServiceOneId;
    @NotEmpty
    String remoteOneData;

    @NotEmpty
    String remoteServiceTwoId;
    @NotEmpty
    String remoteTwoData;

    Integer throwExceptionOnLevel;
}
