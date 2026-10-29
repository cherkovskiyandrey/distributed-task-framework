package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.persistence.repository.TaskExtendedRepository;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class CompletionServiceImplTest {

    private static final Duration REGISTER_TIMEOUT = Duration.ofMillis(1);
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(2);

    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
        .setNameFormat("completion-service-test-%d")
        .build();

    @Mock
    TaskExtendedRepository taskExtendedRepository;
    @Mock
    WorkerContextManager workerContextManager;
    @InjectMocks
    CompletionServiceImpl completionService;

    @Test
    @Timeout(10)
    void waitCompletionAllWorkflow() throws Exception {
        UUID workflowId1 = UUID.randomUUID();
        UUID workflowId2 = UUID.randomUUID();

        CyclicBarrier registeredBarrier = new CyclicBarrier(2);
        CyclicBarrier handleBarrier = new CyclicBarrier(2);

        //arrange: repository call is slow and lets waiter2 register its workflow in the middle of handle()
        Mockito.doAnswer(invocation -> {
            // the snapshot of registered workflows is already taken, so it is safe to register a new workflow right now
            handleBarrier.await();
            // imitate slow repository call: waiter2 registers its workflow while the handler is in progress
            Thread.sleep(500);
            return Set.<UUID>of();
        }).when(taskExtendedRepository).filterExistedWorkflowIds(Set.of(workflowId1));


        ExecutorService executorService = Executors.newFixedThreadPool(3, THREAD_FACTORY);
        try {
            //act
            Future<?> waiter1 = executorService.submit(() -> registerAndWaitWorkflow(workflowId1, registeredBarrier));
            Future<?> handler = executorService.submit(() -> handle(registeredBarrier));
            Future<?> waiter2 = executorService.submit(() -> waitWorkflow(workflowId2, handleBarrier));

            //assert
            assertThatCode(handler::get).doesNotThrowAnyException();
            assertThatCode(waiter1::get).doesNotThrowAnyException();
            assertThatThrownBy(waiter2::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TimeoutException.class);
        } finally {
            executorService.shutdownNow();
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    @SneakyThrows
    private void handle(CyclicBarrier registeredBarrier) {
        registeredBarrier.await();
        completionService.handle();
    }

    @SneakyThrows
    private void registerAndWaitWorkflow(UUID workflowId, CyclicBarrier registeredBarrier) {
        // just register workflow: the handler waits at registeredBarrier, so this call is bound to timeout
        try {
            completionService.waitCompletionAllWorkflow(workflowId, REGISTER_TIMEOUT);
        } catch (TimeoutException expected) {
            // workflow is registered now
        }
        registeredBarrier.await();
        // the handler must complete the already registered workflow
        completionService.waitCompletionAllWorkflow(workflowId, WAIT_TIMEOUT);
    }

    @SneakyThrows
    private void waitWorkflow(UUID workflowId, CyclicBarrier handleBarrier) {
        // the handler is inside handle() now, so the workflow is registered after the snapshot
        // and must not be completed by it: timeout is expected
        handleBarrier.await();
        completionService.waitCompletionAllWorkflow(workflowId, WAIT_TIMEOUT);
    }
}
