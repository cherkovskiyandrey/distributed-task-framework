package com.distributed_task_framework.saga.test.autoconfiguration;

import com.distributed_task_framework.persistence.repository.DltRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.persistence.repository.DlsSagaContextRepository;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.test.autoconfiguration.service.SagaTestUtil;
import com.distributed_task_framework.saga.test.autoconfiguration.test_data.service.SagaMethodProviderService;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.test.autoconfigure.TestDistributedTaskAutoconfiguration;
import com.distributed_task_framework.utils.Postgresql16Initializer;
import com.distributed_task_framework.utils.Signaller;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.distributed_task_framework.saga.services.impl.SagaManagerImpl.INTERNAL_SAGA_MANAGER_TASK_DEF;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

//todo: use this library in order to check it for example in distributed-task-test-application (-)
@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "distributed-task.enabled=true",
        "distributed-task.saga.enabled=true",
        "distributed-task.common.app-name=test"
    }
)
@EnableAutoConfiguration
@ContextConfiguration(
    classes = {
        AdditionalTestConfiguration.class,
        TestDistributedTaskAutoconfiguration.class,
        TestSagaAutoconfiguration.class
    },
    initializers = Postgresql16Initializer.class
)
@FieldDefaults(level = AccessLevel.PRIVATE)
class IntegrationTest {
    @Autowired
    TaskRepository taskRepository;
    @Autowired
    DltRepository dltRepository;
    @Autowired
    WorkerManager workerManager;
    @Autowired
    SagaRepository sagaRepository;
    @Autowired
    DlsSagaContextRepository dlsSagaContextRepository;
    @Autowired
    DistributionSagaService distributionSagaService;
    @Autowired
    SagaTestUtil sagaTestUtil;
    @Autowired
    PlatformTransactionManager transactionManager;
    @Autowired
    Signaller signaller;
    @Autowired
    SagaMethodProviderService sagaMethodProviderService;

    @SneakyThrows
    @Test
    void shouldCancelAllSagasCorrectly() {
        //when
        int maxFLows = 10;
        signaller.reinit(maxFLows + 1);

        var flows = new TransactionTemplate(transactionManager).execute(ts ->
            IntStream.range(0, maxFLows)
                .mapToObj(i ->
                    distributionSagaService.create("test_" + i)
                        .registerToConsume(
                            sagaMethodProviderService::forward,
                            sagaMethodProviderService::backward,
                            i
                        )
                        .start()
                ).toList()
        );

        //do
        signaller.getCyclicBarrierRef().get().await();
        sagaTestUtil.reinitAndWait(5, Duration.ofSeconds(5));

        //verify
        assertThat(workerManager.getCurrentActiveTaskIds()).allMatch(taskId -> Objects.equals(
                taskId.getTaskName(),
                INTERNAL_SAGA_MANAGER_TASK_DEF.getTaskName()
            )
        );
        assertThat(taskRepository.findAll())
            .singleElement()
            .matches(taskEntity -> Objects.equals(
                    taskEntity.getTaskName(),
                    INTERNAL_SAGA_MANAGER_TASK_DEF.getTaskName()
                )
            );
        assertThat(dltRepository.count()).isEqualTo(0L);

        assertThat(sagaRepository.count()).isEqualTo(0L);
        assertThat(dlsSagaContextRepository.count()).isEqualTo(0L);
        assertThat(flows).allSatisfy(flow ->
            assertThatThrownBy(flow::waitCompletion).isInstanceOf(SagaNotFoundException.class)
        );
    }
}