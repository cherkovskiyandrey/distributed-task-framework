package com.distributed_task_framework.service.impl.workers.local.at_least_once;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.persistence.repository.entity.TestBusinessObjectEntity;
import com.distributed_task_framework.service.impl.workers.local.AbstractLocalWorkerIntegrationTests;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.task.TestTaskModelSpec;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    void shouldNotScheduleNewTaskAndNotSaveBusinessObjectWhenExceptionInExecute() {
        //when
        setFixedTime();
        var childTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.schedule(childTestTaskModel.getTaskDef(), ExecutionContext.empty());
                testBusinessObjectRepository.save(TestBusinessObjectEntity.builder().build());
                throw new RuntimeException();
            })
            .build()
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        verifyTaskInNextAttempt(parentTestTaskModel.getTaskId(), parentTestTaskModel.getTaskSettings());
        assertThat(testBusinessObjectRepository.findAll()).singleElement();
    }
}
