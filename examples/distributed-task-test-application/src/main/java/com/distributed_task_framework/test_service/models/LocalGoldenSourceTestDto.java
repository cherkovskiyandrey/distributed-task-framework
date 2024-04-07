package com.distributed_task_framework.test_service.models;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import jakarta.validation.constraints.NotEmpty;

@Value
@Builder
@Jacksonized
public class LocalGoldenSourceTestDto {
    @NotEmpty
    Long id;
    @NotEmpty
    Long version;

    @NotEmpty
    String remoteServiceOneId;
    @NotEmpty
    String remoteOneData;
}
