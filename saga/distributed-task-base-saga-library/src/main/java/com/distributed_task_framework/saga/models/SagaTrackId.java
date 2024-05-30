package com.distributed_task_framework.saga.models;

import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value(staticConstructor = "of")
@Jacksonized
public class SagaTrackId {
    String trackId;
}
