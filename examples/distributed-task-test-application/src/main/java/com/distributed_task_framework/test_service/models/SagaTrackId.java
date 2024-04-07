package com.distributed_task_framework.test_service.models;

import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value(staticConstructor = "of")
@Jacksonized
public class SagaTrackId {
    String trackId;
}
