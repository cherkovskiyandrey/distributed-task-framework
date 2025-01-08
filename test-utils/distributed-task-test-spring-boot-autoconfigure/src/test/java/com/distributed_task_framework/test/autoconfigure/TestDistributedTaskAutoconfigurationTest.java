package com.distributed_task_framework.test.autoconfigure;

import com.distributed_task_framework.Postgresql16Initializer;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.test.autoconfigure.service.DistributedTaskTestUtil;
import com.distributed_task_framework.test.autoconfigure.tasks.CpuIntensiveExampleTask;
import com.distributed_task_framework.test.autoconfigure.tasks.DefaultTask;
import com.distributed_task_framework.test.autoconfigure.tasks.IOExampleTask;
import com.distributed_task_framework.test.autoconfigure.tasks.InfinitiveWorkflowGenerationExampleTask;
import com.distributed_task_framework.test.autoconfigure.tasks.RetryExampleTask;
import com.distributed_task_framework.test.autoconfigure.tasks.SimpleCronCustomizedTask;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.stream.IntStream;

import static com.distributed_task_framework.test.ClusterProviderTestImpl.TEST_NODE_ID;
import static com.distributed_task_framework.test.autoconfigure.tasks.DefaultTask.TASK_DEF;
import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBootTest(
    properties = {
        "distributed-task.enabled=true",
        "distributed-task.common.app-name=test"
    }
)
@EnableAutoConfiguration
@ContextConfiguration(
    classes = {AdditionalTestConfiguration.class, TestDistributedTaskAutoconfiguration.class},
    initializers = Postgresql16Initializer.class
)
@ComponentScan(basePackageClasses = DefaultTask.class)
@FieldDefaults(level = AccessLevel.PRIVATE)
class TestDistributedTaskAutoconfigurationTest {
    @Autowired
    TaskRepository taskRepository;
    @Autowired
    WorkerManager workerManager;
    @Autowired
    ClusterProvider clusterProvider;
    @Autowired
    DistributedTaskService distributedTaskService;
    @Autowired
    DistributedTaskTestUtil distributedTaskTestUtil;
    @Autowired
    PlatformTransactionManager transactionManager;
    @Autowired
    Signaller signaller;

    @Test
    void shouldLoadCustomClusterProvider() {
        assertThat(clusterProvider.nodeId()).isEqualTo(TEST_NODE_ID);
    }

    @Test
    void shouldLoadRealTaskRegistryService() {
        assertThat(distributedTaskService.isTaskRegistered(TASK_DEF)).isTrue();
    }

    @Test
    void shouldNotScheduleCronTask() {
        assertThat(distributedTaskService.getRegisteredTask(SimpleCronCustomizedTask.TASK_DEF))
            .isNotEmpty()
            .get()
            .matches(registeredTask -> !registeredTask.getTaskSettings().hasCron(), "cron");
    }

    @SneakyThrows
    @Test
    void shouldCancelAllTasksCorrectly() {
        //when
        signaller.reinit(5);

        new TransactionTemplate(transactionManager).executeWithoutResult(ts -> {
            try {
                distributedTaskService.schedule(CpuIntensiveExampleTask.TASK_DEF, ExecutionContext.empty());
                distributedTaskService.schedule(
                    InfinitiveWorkflowGenerationExampleTask.TASK_DEF,
                    ExecutionContext.simple(1)
                );
                distributedTaskService.schedule(IOExampleTask.TASK_DEF, ExecutionContext.empty());
                distributedTaskService.schedule(RetryExampleTask.TASK_DEF, ExecutionContext.empty());

                //build workflow in advance
                var firstLevelTaskIds = IntStream.range(0, 100)
                    .mapToObj(i -> {
                            try {
                                return distributedTaskService.schedule(
                                    DefaultTask.TASK_DEF,
                                    ExecutionContext.empty()
                                );
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    ).toList();

                var secondLevelTaskIds = IntStream.range(0, 10)
                    .mapToObj(i -> {
                            try {
                                return distributedTaskService.scheduleJoin(
                                    DefaultTask.TASK_DEF,
                                    ExecutionContext.empty(),
                                    firstLevelTaskIds
                                );
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    ).toList();

                distributedTaskService.scheduleJoin(
                    DefaultTask.TASK_DEF,
                    ExecutionContext.empty(),
                    secondLevelTaskIds
                );

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        //do
        signaller.getCyclicBarrierRef().get().await();
        distributedTaskTestUtil.reinitAndWait(5, Duration.ofSeconds(10));

        //verify
        assertThat(taskRepository.findAllNotDeletedAndNotCanceled()).isEmpty();
        assertThat(workerManager.getCurrentActiveTasks()).isEqualTo(0L);
        assertThat(taskRepository.count()).isEqualTo(0L);
    }
}