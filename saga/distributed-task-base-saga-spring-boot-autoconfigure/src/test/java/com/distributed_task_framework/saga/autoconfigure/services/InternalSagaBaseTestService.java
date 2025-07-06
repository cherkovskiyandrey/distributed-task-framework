package com.distributed_task_framework.saga.autoconfigure.services;

import java.util.concurrent.TimeoutException;

public interface InternalSagaBaseTestService {

    void calculate(String suffix) throws InterruptedException, TimeoutException;

    void setValue(String val);

    String getValue();
}
