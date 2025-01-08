package com.distributed_task_framework.test.autoconfigure;

import lombok.Getter;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

@Getter
public class Signaller {
    private final AtomicReference<CyclicBarrier> cyclicBarrierRef = new AtomicReference<>(new CyclicBarrier(1));

    void reinit(int parties) {
        cyclicBarrierRef.set(new CyclicBarrier(parties));
    }
}
