package com.distributed_task_framework.saga.test.autoconfiguration.test_data.service;

import com.distributed_task_framework.saga.autoconfigure.annotations.SagaMethod;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaRevertMethod;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.utils.Signaller;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.jetbrains.annotations.Nullable;
import org.springframework.stereotype.Service;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class SagaMethodProviderService {
    Signaller signaller;

    @SagaMethod(name = "forward")
    public void forward(int data) throws InterruptedException, BrokenBarrierException {
        signaller.getCyclicBarrierRef().get().await();
        TimeUnit.SECONDS.sleep(100);
    }

    @SagaRevertMethod(name = "backward")
    public void backward(int data, @Nullable SagaExecutionException sagaExecutionException) throws InterruptedException, BrokenBarrierException {
        signaller.getCyclicBarrierRef().get().await();
        TimeUnit.SECONDS.sleep(100);
    }
}
