package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.persistence.repository.TaskExtendedRepository;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.AdditionalAnswers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

@ExtendWith(MockitoExtension.class)
@Slf4j
class CompletionServiceImplTest {

    @Mock
    TaskExtendedRepository taskExtendedRepository;
    @Mock
    WorkerContextManager workerContextManager;
    @InjectMocks
    CompletionServiceImpl completionService;

    @Test
    void waitCompletionAllWorkflow() throws Exception {
        AtomicReference<Throwable> waiter1Timeout = new AtomicReference<>();
        AtomicReference<Throwable> waiter2Timeout = new AtomicReference<>();

        UUID workflowId1 = UUID.randomUUID();
        UUID workflowId2 = UUID.randomUUID();

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch firstWaitLatch = new CountDownLatch(1);
        CountDownLatch handleLatch = new CountDownLatch(1);
        CountDownLatch secondWaitLatch = new CountDownLatch(1);

        Thread waiter1 = new Thread(() -> waitWorkflow(workflowId1, start, firstWaitLatch), "waiter1");
        waiter1.setUncaughtExceptionHandler((t, e) -> waiter1Timeout.set(e));

        Thread handler = new Thread(() -> handle(workflowId1, firstWaitLatch, handleLatch), "handler");

        Thread waiter2 = new Thread(() -> waitWorkflow(workflowId2, handleLatch, secondWaitLatch), "waiter2");
        waiter2.setUncaughtExceptionHandler((t, e) -> waiter2Timeout.set(e));

        waiter1.start();
        handler.start();
        waiter2.start();

        Thread.sleep(100);
        start.countDown();

        waiter1.join();
        waiter2.join();
        handler.join();

        Assertions.assertThat(waiter1Timeout.get()).as("timeout waiter 1 not happened").isNull();
        Assertions.assertThat(waiter2Timeout.get()).as("timeout waiter 2 happened").isNotNull();
    }

    @SneakyThrows
    private void handle(UUID workflow1, CountDownLatch condition, CountDownLatch action) {
        Mockito.doAnswer(AdditionalAnswers.answersWithDelay(500, new Returns(Set.<UUID>of())))
            .when(taskExtendedRepository)
            .filterExistedWorkflowIds(Set.of(workflow1));

        condition.await();
        Thread.sleep(10);

        action.countDown();
        completionService.handle();
    }

    @SneakyThrows
    private void waitWorkflow(UUID workflowId, CountDownLatch condition, CountDownLatch action) {
        Duration expectedWaitDuration = Duration.ofSeconds(1);

        condition.await();
        Thread.sleep(10);
        action.countDown();
        completionService.waitCompletionAllWorkflow(workflowId, expectedWaitDuration);
    }
}