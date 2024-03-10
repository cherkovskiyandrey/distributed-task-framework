package com.distributed_task_framework.service.impl;

import lombok.Builder;
import lombok.Value;

import java.util.concurrent.atomic.AtomicInteger;

@Value
@Builder
public class DeliveryLoopSignals {
    AtomicInteger startSignal;
    AtomicInteger stopSignal;
}
