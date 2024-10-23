package com.distributed_task_framework.saga.test_service.models;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class RemoteOneDto {
    String remoteOneId;
    String remoteOneData;
}
