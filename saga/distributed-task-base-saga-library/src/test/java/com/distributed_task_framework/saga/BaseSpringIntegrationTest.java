package com.distributed_task_framework.saga;

import com.distributed_task_framework.saga.generator.TestSagaGenerator;
import com.distributed_task_framework.saga.generator.TestSagaResolvingGenerator;
import com.distributed_task_framework.saga.mappers.SagaMapper;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.services.impl.SagaManagerImpl;
import com.distributed_task_framework.saga.services.impl.SagaResolverImpl;
import com.distributed_task_framework.saga.services.impl.SagaStatService;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.service.internal.DistributedTaskMetricHelper;
import com.distributed_task_framework.service.internal.PlannerService;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;
import com.distributed_task_framework.utils.MetricTestHelper;
import com.distributed_task_framework.utils.Postgresql16Initializer;
import com.distributed_task_framework.utils.TestClock;
import io.micrometer.core.instrument.MeterRegistry;
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
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.Callable;

import static com.distributed_task_framework.saga.services.impl.SagaManagerImpl.INTERNAL_SAGA_MANAGER_TASK_DEF;
import static com.distributed_task_framework.test.autoconfigure.service.impl.DistributedTaskTestUtilImpl.DEFAULT_ATTEMPTS;
import static com.distributed_task_framework.test.autoconfigure.service.impl.DistributedTaskTestUtilImpl.DEFAULT_DURATION;
import static org.awaitility.Awaitility.await;

@Disabled
@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "spring.datasource.hikari.minimum-idle=10",
        "distributed-task.enabled=true",
        "distributed-task.common.app-name=saga-test"
        //other dtf params is default and from test-utils/distributed-task-test-spring-boot-autoconfigure/src/main/resources/application-dtf-test-utils.yml
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
    SagaManagerImpl sagaManager;
    @Autowired
    SagaTaskFactory sagaTaskFactory;
    @Autowired
    SagaResolverImpl sagaResolver;
    @Autowired
    SagaStatService sagaStatService;
    @Autowired
    SagaMapper sagaMapper;
    @Autowired
    MetricTestHelper metricTestHelper;
    @Autowired
    DistributedTaskMetricHelper distributedTaskMetricHelper;
    @Autowired
    MeterRegistry meterRegistry;
    @SpyBean(name = "virtualQueueManagerPlanner")
    PlannerService plannerService;

    @SneakyThrows
    @BeforeEach
    @AfterEach
    public void init() {
        Assertions.setMaxStackTraceElementsDisplayed(100);
        clock.setClock(Clock.systemUTC());
        distributedTaskTestUtil.reinitAndWait(
            DEFAULT_ATTEMPTS,
            DEFAULT_DURATION,
            List.of(INTERNAL_SAGA_MANAGER_TASK_DEF)
        );
        sagaRepository.deleteAll();
        testSagaGenerator.reset();
        Mockito.reset(sagaTaskFactory);
    }

    protected void setFixedTime() {
        clock.setClock(Clock.fixed(Instant.ofEpochSecond(0), ZoneId.of("UTC")));
    }

    protected void setFixedTime(long secAfterBeginOfEpoch) {
        clock.setClock(Clock.fixed(Instant.ofEpochSecond(secAfterBeginOfEpoch), ZoneId.of("UTC")));
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

        @Bean
        public MetricTestHelper metricTestHelper(MeterRegistry meterRegistry) {
            return new MetricTestHelper(meterRegistry);
        }
    }
}
