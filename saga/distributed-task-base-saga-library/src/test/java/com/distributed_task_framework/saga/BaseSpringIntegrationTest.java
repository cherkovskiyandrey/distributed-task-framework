package com.distributed_task_framework.saga;

import com.distributed_task_framework.Postgresql16Initializer;
import com.distributed_task_framework.TestClock;
import com.distributed_task_framework.saga.generator.TestSagaGenerator;
import com.distributed_task_framework.saga.generator.TestSagaResolvingGenerator;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.services.impl.SagaResolverImpl;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.Callable;

import static org.awaitility.Awaitility.await;

@Disabled
@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "spring.datasource.hikari.minimum-idle=10",
        "distributed-task.enabled=true",
        "distributed-task.common.app-name=saga-test",
        "distributed-task.common.planner.watchdog-initial-delay-ms=100",
        "distributed-task.common.planner.watchdog-fixed-delay-ms=100",
        "distributed-task.common.planner.polling-delay.0=100",
        "distributed-task.common.registry.update-initial-delay-ms=100",
        "distributed-task.common.registry.update-fixed-delay-ms=100",
        "distributed-task.common.worker-manager.max-parallel-tasks-in-node=10",
    }
)
@EnableAutoConfiguration
@ContextConfiguration(
    initializers = {Postgresql16Initializer.class},
    classes = {
        BaseTestConfiguration.class,
        BaseSpringIntegrationTest.AdditionalTestConfiguration.class
    }
)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class BaseSpringIntegrationTest {
    @Autowired
    TestClock clock;
    @Autowired
    DistributedTaskTestUtil distributedTaskTestUtil;
    @Autowired
    DistributionSagaService distributionSagaService;
    @Autowired
    SagaRepository sagaRepository;
    @Autowired
    TestSagaGenerator testSagaGenerator;
    @Autowired
    TestSagaResolvingGenerator testSagaResolvingGenerator;
    @Autowired
    SagaManager sagaManager;
    @Autowired
    SagaTaskFactory sagaTaskFactory;
    @Autowired
    SagaResolverImpl sagaResolver;

    @SneakyThrows
    @BeforeEach
    @AfterEach
    public void init() {
        Assertions.setMaxStackTraceElementsDisplayed(100);
        clock.setClock(Clock.systemUTC());
        distributedTaskTestUtil.reinitAndWait();
        sagaRepository.deleteAll();
        testSagaGenerator.reset();
        Mockito.reset(sagaTaskFactory);
    }

    protected void setFixedTime() {
        clock.setClock(Clock.fixed(Instant.ofEpochSecond(0), ZoneId.of("UTC")));
    }

    protected void waitFor(Callable<Boolean> conditionEvaluator) {
        await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofMillis(500))
            .until(conditionEvaluator);
    }

    @TestConfiguration
    public static class AdditionalTestConfiguration {

        @Bean
        public TestSagaGenerator taskPopulate(DistributionSagaService distributionSagaService,
                                              SagaResolver sagaResolver,
                                              SagaTaskFactory sagaTaskFactory) {
            return new TestSagaGenerator(
                distributionSagaService,
                sagaResolver,
                sagaTaskFactory
            );
        }

        @Bean
        public TestSagaResolvingGenerator testSagaResolvingGenerator(SagaResolver sagaResolver) {
            return new TestSagaResolvingGenerator(sagaResolver);
        }
    }
}
