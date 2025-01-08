package com.distributed_task_framework.saga.test_service.services;

import com.distributed_task_framework.saga.test_service.models.TestDataDto;
import com.distributed_task_framework.saga.test_service.persistence.entities.Audit;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public interface TestSagaService {

    Audit naiveSagaCall(TestDataDto testDataDto);

    void sagaCallAsyncWithoutTrackId(TestDataDto testDataDto);

    void runSagaSync(TestDataDto testDataDto) throws InterruptedException, TimeoutException;

    Audit sagaCall(TestDataDto testDataDto);

    UUID sagaCallAsync(TestDataDto testDataDto);

    void cancel(UUID sagaId, boolean gracefully);

    Optional<Audit> sagaCallPollResult(UUID trackId);
}
