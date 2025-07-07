package com.distributed_task_framework.saga.autoconfigure.test_data.services;

import java.util.concurrent.TimeoutException;

public interface InternalSagaBaseTestService {

    void calculate(String suffix) throws InterruptedException, TimeoutException;

    void setValue(String val);

    String getValue();
}
