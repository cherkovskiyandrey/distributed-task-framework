package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.exception.InvalidOperationException;
import com.distributed_task_framework.task.TestTaskModelCustomizerUtils;
import com.distributed_task_framework.task.TestTaskModelSpec;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractWaitCompletionAllWorkflowTest extends BaseLocalWorkerIntegrationTest {

    @Test
    void shouldProhibitedToCallWaitCompletionAllWorkflowWhenFromTask() {
        //when
        var testTaskModel = extendedTaskGenerator.generate(TestTaskModelCustomizerUtils.assigned(String.class));
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                //verify
                assertThatThrownBy(() -> distributedTaskService.waitCompletionAllWorkflow(testTaskModel.getTaskId()))
                    .isInstanceOf(InvalidOperationException.class);
            })
            .build()
        );

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());
    }

    @Test
    void shouldProhibitedToCallWaitCompletionAllWorkflowWhenFromTaskWithTimeout() {
        //when
        var testTaskModel = extendedTaskGenerator.generate(TestTaskModelCustomizerUtils.assigned(String.class));
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                //verify
                assertThatThrownBy(() -> distributedTaskService.waitCompletionAllWorkflow(
                        testTaskModel.getTaskId(),
                        Duration.ofSeconds(1)
                    )
                ).isInstanceOf(InvalidOperationException.class);
            })
            .build()
        );

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());
    }
}
