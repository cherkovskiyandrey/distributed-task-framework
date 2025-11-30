package com.distributed_task_framework.saga.autoconfigure;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@FieldDefaults(level = AccessLevel.PRIVATE)
public class SimpleTest extends BaseSpringIntegrationTest {

    @Test
    void shouldStartupContext() {
        assertThat(distributionSagaService).isNotNull();
    }
}
