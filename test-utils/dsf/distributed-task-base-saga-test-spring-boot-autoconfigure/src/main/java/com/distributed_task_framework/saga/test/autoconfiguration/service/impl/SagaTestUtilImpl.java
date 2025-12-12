package com.distributed_task_framework.saga.test.autoconfiguration.service.impl;

import com.distributed_task_framework.saga.persistence.repository.DlsSagaContextRepository;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.test.autoconfiguration.service.SagaTestUtil;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;

import static com.distributed_task_framework.saga.services.impl.SagaManagerImpl.INTERNAL_SAGA_MANAGER_TASK_DEF;
import static com.distributed_task_framework.test.autoconfigure.service.impl.DistributedTaskTestUtilImpl.DEFAULT_ATTEMPTS;
import static com.distributed_task_framework.test.autoconfigure.service.impl.DistributedTaskTestUtilImpl.DEFAULT_DURATION;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaTestUtilImpl implements SagaTestUtil {
    DistributedTaskTestUtil distributedTaskTestUtil;
    SagaRepository sagaRepository;
    DlsSagaContextRepository dlsSagaContextRepository;


    @Override
    public void reinitAndWait() throws InterruptedException {
        log.info("reinitAndWait(): begin");
        distributedTaskTestUtil.reinitAndWait(
            DEFAULT_ATTEMPTS,
            DEFAULT_DURATION,
            List.of(INTERNAL_SAGA_MANAGER_TASK_DEF)
        );
        sagaRepository.deleteAll();
        dlsSagaContextRepository.deleteAll();
        log.info("reinitAndWait(): end");
    }

    @Override
    public void reinitAndWait(int attemptsToCancel, Duration duration) throws InterruptedException {
        log.info("reinitAndWait(): begin");
        distributedTaskTestUtil.reinitAndWait(
            attemptsToCancel,
            duration,
            List.of(INTERNAL_SAGA_MANAGER_TASK_DEF)
        );
        sagaRepository.deleteAll();
        dlsSagaContextRepository.deleteAll();
        log.info("reinitAndWait(): end");
    }
}
