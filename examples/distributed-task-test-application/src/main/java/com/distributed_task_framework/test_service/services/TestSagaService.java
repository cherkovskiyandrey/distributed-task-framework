package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.test_service.models.TestDataDto;
import com.distributed_task_framework.test_service.models.SagaTrackId;
import com.distributed_task_framework.test_service.persistence.entities.Audit;

import java.util.Optional;
import java.util.concurrent.TimeoutException;

public interface TestSagaService {

    Audit naiveSagaCall(TestDataDto testDataDto);

    void sagaCallAsyncWithoutTrackId(TestDataDto testDataDto);

    void runSagaSync(TestDataDto testDataDto) throws InterruptedException, TimeoutException;

    Audit sagaCall(TestDataDto testDataDto);

    SagaTrackId sagaCallAsync(TestDataDto testDataDto);

    Optional<Audit> sagaCallPollResult(SagaTrackId trackId);
}
