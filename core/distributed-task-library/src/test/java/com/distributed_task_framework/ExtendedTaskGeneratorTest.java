package com.distributed_task_framework;

import com.distributed_task_framework.task.TestTaskModel;
import com.distributed_task_framework.task.TestTaskModelSpec;
import org.junit.jupiter.api.Test;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple tests for generator.
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class ExtendedTaskGeneratorTest extends BaseSpringIntegrationTest {

    @Test
    void shouldUseTestModel() {
        //when
        TestTaskModel<String> testTaskModel = extendedTaskGenerator.generate(
            TestTaskModelSpec.builder(String.class)
                .withSaveInstance()
                .build()
        );
        //verify
        assertThat(testTaskModel.getTaskDef()).isNotNull();
        assertThat(testTaskModel.getTaskId()).isNotNull();
        assertThat(testTaskModel.getTaskSettings()).isNotNull();
        assertThat(testTaskModel.getTaskEntity()).isNotNull();
        assertThat(testTaskModel.getRegisteredTask()).isNotNull();
    }
}
