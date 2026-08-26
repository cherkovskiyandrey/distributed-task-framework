package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.interceptor.CommonTaskCreationInterceptor;
import com.distributed_task_framework.interceptor.CommonTaskExecutionInterceptor;
import com.distributed_task_framework.interceptor.TaskCreationInterceptor;
import com.distributed_task_framework.interceptor.TaskExecutionChain;
import com.distributed_task_framework.interceptor.TaskExecutionInterceptor;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.Metadata;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.DltEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.TestStatefulTaskModelSpec;
import com.distributed_task_framework.task.TestTaskModelSpec;
import com.distributed_task_framework.utils.TaskGenerator;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.Order;
import org.springframework.test.annotation.DirtiesContext;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
@Import(MetadataIntegrationTest.InterceptorsTestConfiguration.class)
class MetadataIntegrationTest extends BaseSpringIntegrationTest {
    static final ConcurrentLinkedQueue<String> EXECUTION_SEQUENCE = new ConcurrentLinkedQueue<>();
    static final ConcurrentLinkedQueue<String> COMMON_EXECUTION_SEQUENCE = new ConcurrentLinkedQueue<>();

    //turn off background workers and registry
    @MockBean
    WorkerManager workerManager;
    @MockBean
    TaskRegistryService taskRegistryService;

    @Autowired
    @Qualifier("localAtLeastOnceWorker")
    TaskWorker taskWorker;
    @Autowired
    DistributedTaskService distributedTaskService;

    @BeforeEach
    public void init() {
        super.init();
        RecordingCreationInterceptor.INVOCATIONS.clear();
        RecordingCreationInterceptorTwo.INVOCATIONS.clear();
        CommonRecordingCreationInterceptor.INVOCATIONS.clear();
        CommonRecordingCreationInterceptorTwo.INVOCATIONS.clear();
        COMMON_EXECUTION_SEQUENCE.clear();
        EXECUTION_SEQUENCE.clear();
    }

    @TestConfiguration
    public static class InterceptorsTestConfiguration {

        @Bean
        public RecordingCreationInterceptor recordingCreationInterceptor() {
            return new RecordingCreationInterceptor();
        }

        @Bean
        public RecordingCreationInterceptorTwo recordingCreationInterceptorTwo() {
            return new RecordingCreationInterceptorTwo();
        }

        @Bean
        public FirstOrderedExecutionInterceptor firstOrderedExecutionInterceptor() {
            return new FirstOrderedExecutionInterceptor();
        }

        @Bean
        public SecondOrderedExecutionInterceptor secondOrderedExecutionInterceptor() {
            return new SecondOrderedExecutionInterceptor();
        }

        @Bean
        public CommonRecordingCreationInterceptor commonRecordingCreationInterceptor() {
            return new CommonRecordingCreationInterceptor();
        }

        @Bean
        public CommonRecordingCreationInterceptorTwo commonRecordingCreationInterceptorTwo() {
            return new CommonRecordingCreationInterceptorTwo();
        }

        @Bean
        public CommonRecordingExecutionInterceptor commonRecordingExecutionInterceptor() {
            return new CommonRecordingExecutionInterceptor();
        }
    }

    public record CreationInvocation(ExecutionContext<?> executionContext, TaskDef<?> taskDef) {
    }

    public static class RecordingCreationInterceptor implements TaskCreationInterceptor {
        public static final ConcurrentLinkedQueue<CreationInvocation> INVOCATIONS = new ConcurrentLinkedQueue<>();

        @Override
        public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
            INVOCATIONS.add(new CreationInvocation(executionContext, taskDef));
            return executionContext.withMetadata(executionContext.getMetadata().add("createdBy", "interceptor"));
        }
    }

    public static class RecordingCreationInterceptorTwo implements TaskCreationInterceptor {
        public static final ConcurrentLinkedQueue<CreationInvocation> INVOCATIONS = new ConcurrentLinkedQueue<>();

        @Override
        public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
            INVOCATIONS.add(new CreationInvocation(executionContext, taskDef));
            return executionContext;
        }
    }

    public static class CommonRecordingCreationInterceptor implements CommonTaskCreationInterceptor {
        public static final ConcurrentLinkedQueue<CreationInvocation> INVOCATIONS = new ConcurrentLinkedQueue<>();

        @Override
        public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
            INVOCATIONS.add(new CreationInvocation(executionContext, taskDef));
            return executionContext;
        }
    }

    public static class CommonRecordingCreationInterceptorTwo implements CommonTaskCreationInterceptor {
        public static final ConcurrentLinkedQueue<CreationInvocation> INVOCATIONS = new ConcurrentLinkedQueue<>();

        @Override
        public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
            INVOCATIONS.add(new CreationInvocation(executionContext, taskDef));
            return executionContext;
        }
    }

    public static class CommonRecordingExecutionInterceptor implements CommonTaskExecutionInterceptor {
        @Override
        public <U> void execute(ExecutionContext<U> executionContext, TaskDef<U> taskDef, TaskExecutionChain chain) throws Exception {
            COMMON_EXECUTION_SEQUENCE.add("before-common");
            chain.proceed();
            COMMON_EXECUTION_SEQUENCE.add("after-common");
        }
    }

    @Order(1)
    public static class FirstOrderedExecutionInterceptor implements TaskExecutionInterceptor {
        @Override
        public <U> void execute(ExecutionContext<U> executionContext, TaskDef<U> taskDef, TaskExecutionChain chain) throws Exception {
            EXECUTION_SEQUENCE.add("before-first");
            chain.proceed();
            EXECUTION_SEQUENCE.add("after-first");
        }
    }

    @Order(2)
    public static class SecondOrderedExecutionInterceptor implements TaskExecutionInterceptor {
        @Override
        public <U> void execute(ExecutionContext<U> executionContext, TaskDef<U> taskDef, TaskExecutionChain chain) throws Exception {
            EXECUTION_SEQUENCE.add("before-second");
            chain.proceed();
            EXECUTION_SEQUENCE.add("after-second");
        }
    }

    @SneakyThrows
    @Test
    void shouldProvideApiMetadataAtExecution() {
        //when
        ConcurrentLinkedQueue<ExecutionContext<String>> executedContexts = new ConcurrentLinkedQueue<>();
        var taskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .action(executedContexts::add)
            .build()
        );
        Metadata metadata = Metadata.of("traceId", "abc-123");

        //do
        TaskId taskId = distributedTaskService.schedule(
            taskModel.getTaskDef(),
            executionContext("hello", metadata)
        );

        //verify persistence
        TaskEntity taskEntity = taskRepository.find(taskId.getId()).orElseThrow();
        assertThat(readPersistedMetadata(taskEntity)).isEqualTo(metadata);

        //do execution
        taskWorker.execute(taskEntity, taskModel.getRegisteredTask());

        //verify
        assertThat(executedContexts).singleElement().satisfies(ctx ->
            assertThat(ctx.getMetadata()).isEqualTo(metadata)
        );
    }

    @SneakyThrows
    @Test
    void shouldApplyCreationInterceptorMetadata() {
        //when
        ConcurrentLinkedQueue<ExecutionContext<String>> executedContexts = new ConcurrentLinkedQueue<>();
        var taskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .action(executedContexts::add)
            .taskSetting(settings -> settings.toBuilder()
                .creationInterceptors(List.of(RecordingCreationInterceptor.class))
                .build())
            .build()
        );

        //do
        TaskId taskId = distributedTaskService.schedule(taskModel.getTaskDef(), executionContext("hello", Metadata.empty()));

        //verify interceptor has been invoked with the task definition in scope
        assertThat(RecordingCreationInterceptor.INVOCATIONS).singleElement().satisfies(invocation ->
            assertThat(invocation.taskDef()).isEqualTo(taskModel.getTaskDef())
        );

        //verify metadata added by the interceptor has been persisted
        TaskEntity taskEntity = taskRepository.find(taskId.getId()).orElseThrow();
        assertThat(readPersistedMetadata(taskEntity).get("createdBy")).isEqualTo(List.of("interceptor"));

        //do execution
        taskWorker.execute(taskEntity, taskModel.getRegisteredTask());

        //verify metadata is available at execution
        assertThat(executedContexts).singleElement().satisfies(ctx ->
            assertThat(ctx.getMetadata().get("createdBy")).isEqualTo(List.of("interceptor"))
        );
    }

    @SneakyThrows
    @Test
    void shouldNotApplyCreationInterceptorsWhenNotSelected() {
        //when
        var taskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .action(ctx -> {
            })
            .build()
        );

        //do
        TaskId taskId = distributedTaskService.schedule(taskModel.getTaskDef(), executionContext("hello", Metadata.empty()));

        //verify
        assertThat(RecordingCreationInterceptor.INVOCATIONS).isEmpty();
        assertThat(RecordingCreationInterceptorTwo.INVOCATIONS).isEmpty();
        TaskEntity taskEntity = taskRepository.find(taskId.getId()).orElseThrow();
        assertThat(taskEntity.getMetadataBytes()).isNull();
    }

    @SneakyThrows
    @Test
    void shouldWrapExecutionWithInterceptorsInOrder() {
        //when
        var taskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .action(ctx -> EXECUTION_SEQUENCE.add("task"))
            .taskSetting(settings -> settings.toBuilder()
                .executionInterceptors(List.of(
                    FirstOrderedExecutionInterceptor.class,
                    SecondOrderedExecutionInterceptor.class
                ))
                .build())
            .build()
        );
        TaskId taskId = distributedTaskService.schedule(taskModel.getTaskDef(), executionContext("hello", Metadata.empty()));
        TaskEntity taskEntity = taskRepository.find(taskId.getId()).orElseThrow();

        //do
        taskWorker.execute(taskEntity, taskModel.getRegisteredTask());

        //verify
        assertThat(EXECUTION_SEQUENCE).containsExactly(
            "before-first",
            "before-second",
            "task",
            "after-second",
            "after-first"
        );
    }

    @SneakyThrows
    @Test
    void shouldWrapStatefulTaskExecutionWithInterceptors() {
        //when
        ConcurrentLinkedQueue<Metadata> capturedMetadata = new ConcurrentLinkedQueue<>();
        var statefulTaskModel = extendedTaskGenerator.generate(TestStatefulTaskModelSpec.builder(String.class, String.class)
            .action((ctx, holder) -> capturedMetadata.add(ctx.getMetadata()))
            .taskSetting(settings -> settings.toBuilder()
                .executionInterceptors(List.of(FirstOrderedExecutionInterceptor.class))
                .build())
            .build()
        );
        Metadata metadata = Metadata.of("traceId", "stateful-1");
        TaskId taskId = distributedTaskService.schedule(
            statefulTaskModel.getTaskDef(),
            executionContext("hello", metadata)
        );
        TaskEntity taskEntity = taskRepository.find(taskId.getId()).orElseThrow();

        //do
        taskWorker.execute(taskEntity, statefulTaskModel.getRegisteredTask());

        //verify
        assertThat(capturedMetadata).singleElement().isEqualTo(metadata);
        assertThat(EXECUTION_SEQUENCE).containsExactly("before-first", "after-first");
    }

    @SneakyThrows
    @Test
    void shouldInheritMetadataToChildTask() {
        //when
        ConcurrentLinkedQueue<ExecutionContext<String>> childContexts = new ConcurrentLinkedQueue<>();
        var childTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .action(childContexts::add)
            .build()
        );
        TaskDef<String> taskDef = childTaskModel.getTaskDef();
        AtomicReference<TaskId> childTaskIdRef = new AtomicReference<>();
        var parentTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(taskDef)
            .action(ctx -> childTaskIdRef.set(distributedTaskService.schedule(taskDef, ctx.withNewMessage("child"))))
            .build()
        );
        Metadata metadata = Metadata.of("traceId", "flow-1");

        //do
        TaskId parentTaskId = distributedTaskService.schedule(taskDef, executionContext("parent", metadata));
        TaskEntity parentTaskEntity = taskRepository.find(parentTaskId.getId()).orElseThrow();
        taskWorker.execute(parentTaskEntity, parentTaskModel.getRegisteredTask());

        //verify child has been created and inherits metadata
        TaskEntity childTaskEntity = taskRepository.find(childTaskIdRef.get().getId()).orElseThrow();
        assertThat(readPersistedMetadata(childTaskEntity)).isEqualTo(metadata);
        taskWorker.execute(childTaskEntity, childTaskModel.getRegisteredTask());
        assertThat(childContexts).singleElement().satisfies(ctx ->
            assertThat(ctx.getMetadata()).isEqualTo(metadata)
        );
    }

    @SneakyThrows
    @Test
    void shouldKeepMetadataOnRetry() {
        //when
        AtomicInteger attempts = new AtomicInteger();
        ConcurrentLinkedQueue<Metadata> capturedMetadata = new ConcurrentLinkedQueue<>();
        var taskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .action(ctx -> {
                capturedMetadata.add(ctx.getMetadata());
                if (attempts.getAndIncrement() == 0) {
                    throw new RuntimeException("first attempt fails");
                }
            })
            .build()
        );
        Metadata metadata = Metadata.of("traceId", "retry-1");
        TaskId taskId = distributedTaskService.schedule(taskModel.getTaskDef(), executionContext("hello", metadata));
        TaskEntity taskEntity = taskRepository.find(taskId.getId()).orElseThrow();

        //do
        taskWorker.execute(taskEntity, taskModel.getRegisteredTask());
        TaskEntity retriedTaskEntity = taskRepository.find(taskId.getId()).orElseThrow();
        assertThat(retriedTaskEntity.getFailures()).isEqualTo(1);
        taskWorker.execute(retriedTaskEntity, taskModel.getRegisteredTask());

        //verify metadata is the same on both attempts
        assertThat(capturedMetadata).containsExactly(metadata, metadata);
    }

    @SneakyThrows
    @Test
    void shouldCopyMetadataToDlt() {
        //when
        var taskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .action(ctx -> {
                throw new RuntimeException("permanent fail");
            })
            .failureAction(ctx -> true)
            .build()
        );
        Metadata metadata = Metadata.of("traceId", "dlt-1");
        TaskId taskId = distributedTaskService.schedule(taskModel.getTaskDef(), executionContext("hello", metadata));
        TaskEntity taskEntity = taskRepository.find(taskId.getId()).orElseThrow();

        //do
        taskWorker.execute(taskEntity, taskModel.getRegisteredTask());

        //verify
        DltEntity dltEntity = waitAndGet(
            () -> Lists.newArrayList(dltRepository.findAll()),
            dltEntities -> !dltEntities.isEmpty()
        ).get(0);
        assertThat(readPersistedMetadata(dltEntity)).isEqualTo(metadata);
    }

    @SneakyThrows
    @Test
    void shouldApplyCommonInterceptorsWithoutConfig() {
        //when
        var taskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .action(ctx -> {
            })
            .build()
        );
        TaskId taskId = distributedTaskService.schedule(taskModel.getTaskDef(), executionContext("hello", Metadata.empty()));
        TaskEntity taskEntity = taskRepository.find(taskId.getId()).orElseThrow();

        //do
        taskWorker.execute(taskEntity, taskModel.getRegisteredTask());

        //verify common interceptors have been applied without any config
        assertThat(CommonRecordingCreationInterceptor.INVOCATIONS).singleElement().satisfies(invocation ->
            assertThat(invocation.taskDef()).isEqualTo(taskModel.getTaskDef())
        );
        assertThat(COMMON_EXECUTION_SEQUENCE).containsExactly("before-common", "after-common");
    }

    @SneakyThrows
    @Test
    void shouldExcludeCommonInterceptorsPerTask() {
        //when
        var taskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .action(ctx -> {
            })
            .taskSetting(settings -> settings.toBuilder()
                .excludedCommonCreationInterceptors(List.of(CommonRecordingCreationInterceptor.class))
                .excludedCommonExecutionInterceptors(List.of(CommonRecordingExecutionInterceptor.class))
                .build())
            .build()
        );
        TaskId taskId = distributedTaskService.schedule(taskModel.getTaskDef(), executionContext("hello", Metadata.empty()));
        TaskEntity taskEntity = taskRepository.find(taskId.getId()).orElseThrow();

        //do
        taskWorker.execute(taskEntity, taskModel.getRegisteredTask());

        //verify excluded common interceptors have not been applied, others have
        assertThat(CommonRecordingCreationInterceptor.INVOCATIONS).isEmpty();
        assertThat(CommonRecordingCreationInterceptorTwo.INVOCATIONS).hasSize(1);
        assertThat(COMMON_EXECUTION_SEQUENCE).isEmpty();
    }

    @SneakyThrows
    @Test
    void shouldSkipCreationInterceptorsForClusterOnlyTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("cluster-only-task", String.class);
        ConcurrentLinkedQueue<ExecutionContext<String>> executedContexts = new ConcurrentLinkedQueue<>();
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        RegisteredTask<String> registeredTask = RegisteredTask.of(
            TaskGenerator.defineTask(taskDef, executedContexts::add),
            taskSettings
        );
        when(taskRegistryService.getRegisteredTask(taskDef)).thenReturn(Optional.empty());
        when(taskRegistryService.hasClusterRegisteredTaskByName(taskDef.getTaskName())).thenReturn(true);
        Metadata metadata = Metadata.of("traceId", "cluster-1");

        //do
        TaskId taskId = distributedTaskService.scheduleUnsafe(taskDef, executionContext("hello", metadata));

        //verify configured creation interceptors have not been invoked but common ones have
        assertThat(RecordingCreationInterceptor.INVOCATIONS).isEmpty();
        assertThat(RecordingCreationInterceptorTwo.INVOCATIONS).isEmpty();
        assertThat(CommonRecordingCreationInterceptor.INVOCATIONS).hasSize(1);
        TaskEntity taskEntity = taskRepository.find(taskId.getId()).orElseThrow();
        assertThat(readPersistedMetadata(taskEntity)).isEqualTo(metadata);

        //do execution
        taskWorker.execute(taskEntity, registeredTask);

        //verify metadata is available at execution
        assertThat(executedContexts).singleElement().satisfies(ctx ->
            assertThat(ctx.getMetadata()).isEqualTo(metadata)
        );
    }

    private ExecutionContext<String> executionContext(String message, Metadata metadata) {
        return ExecutionContext.<String>builder()
            .workflowId(UUID.randomUUID())
            .inputMessage(message)
            .metadata(metadata)
            .build();
    }

    @SneakyThrows
    private Metadata readPersistedMetadata(TaskEntity taskEntity) {
        return taskSerializer.readValue(taskEntity.getMetadataBytes(), Metadata.class);
    }

    @SneakyThrows
    private Metadata readPersistedMetadata(DltEntity dltEntity) {
        return taskSerializer.readValue(dltEntity.getMetadataBytes(), Metadata.class);
    }
}
