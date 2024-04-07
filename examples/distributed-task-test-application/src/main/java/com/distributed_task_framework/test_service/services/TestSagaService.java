package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.test_service.models.TestDataDto;
import com.distributed_task_framework.test_service.models.SagaTrackId;
import com.distributed_task_framework.test_service.persistence.entities.Audit;

import java.util.Optional;

public interface TestSagaService {

    Audit naiveSagaCall(TestDataDto testDataDto);

    void sagaCallAsyncWithoutTrackId(TestDataDto testDataDto);

    Audit sagaCall(TestDataDto testDataDto);

    SagaTrackId sagaCallAsync(TestDataDto testDataDto);

    Optional<Audit> sagaCallPollResult(SagaTrackId trackId);
}
