package com.distributed_task_framework.perf_test;

import com.distributed_task_framework.perf_test.persistence.entity.PerfTestState;
import com.distributed_task_framework.perf_test.service.PerfTestService;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestGeneratedSpecDto;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;
import com.distributed_task_framework.utils.Postgresql16Initializer;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

//2. Try to rewrite test-utils on testFixture approach if possible (-)
@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "distributed-task.enabled=true",
        "distributed-task.common.app-name=test"
    }
)
@ContextConfiguration(
    initializers = Postgresql16Initializer.class
)
@FieldDefaults(level = AccessLevel.PRIVATE)
class PerfTestServiceIntegrationTest {
    @Autowired
    PerfTestService perfTestService;
    @Autowired
    DistributedTaskTestUtil distributedTaskTestUtil;

    @SneakyThrows
    @BeforeEach
    public void init() {
        distributedTaskTestUtil.reinitAndWait();
    }

    @SneakyThrows
    @Test
    void shouldRunAndCompletePerfTest() {
        //when
        var testName = "test";
        var spec = PerfTestGeneratedSpecDto.builder()
            .name(testName)
            .affinityGroup("test-afg")
            .totalPipelines(2)
            .totalAffinities(1)
            .totalTaskOnFirstLevel(1)
            .totalTaskOnSecondLevel(1)
            .taskDurationMs(1)
            .build();

        //do
        perfTestService.run(spec);

        //verify
        var stat = await()
            .atMost(Duration.ofMinutes(1))
            .pollInterval(Duration.ofSeconds(1))
            .until(
                () -> perfTestService.stat(testName),
                st -> !st.getSummaryStates().isEmpty()
                    && st.getSummaryStates().keySet().stream().allMatch(PerfTestState.DONE::equals)
            );
        assertThat(stat)
            .matches(st -> st.getDuration() != null, "duration")
            .matches(st -> st.getTotalPipelines() == 2, "totalPipelines");
    }
}