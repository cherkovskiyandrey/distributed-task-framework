package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.saga.services.DistributionSagaService;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

@FieldDefaults(level = AccessLevel.PRIVATE)
public class SimpleTest extends BaseSpringIntegrationTest {

    @Autowired
    DistributionSagaService distributionSagaService;

    @Test
    void shouldStartupContext() {
        assertThat(distributionSagaService).isNotNull();
    }
}
