package com.distributed_task_framework.service.impl.workers.local.at_least_once;

import com.distributed_task_framework.service.impl.workers.local.AbstractLocalWorkerIntegrationTests;
import com.distributed_task_framework.service.internal.TaskWorker;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
public class AtLeastOnceLocalWorkerIntegrationTest extends AbstractLocalWorkerIntegrationTests {
    @Autowired
    @Qualifier("localAtLeastOnceWorker")
    TaskWorker taskWorker;

    @Override
    protected TaskWorker getTaskWorker() {
        return taskWorker;
    }
}
