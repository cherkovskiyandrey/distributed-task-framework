package com.distributed_task_framework.test_service.models;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class RemoteTwoDto {
    String remoteTwoId;
    String remoteTwoData;
    String remoteOneId;
}
