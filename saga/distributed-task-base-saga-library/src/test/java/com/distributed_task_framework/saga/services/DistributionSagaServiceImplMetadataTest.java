package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.Metadata;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.models.SagaAction;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.impl.DistributionSagaServiceImpl;
import com.distributed_task_framework.saga.services.impl.SagaHelper;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.service.DistributedTaskService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.PlatformTransactionManager;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DistributionSagaServiceImplMetadataTest {

    @Mock
    PlatformTransactionManager transactionManager;
    @Mock
    SagaResolver sagaResolver;
    @Mock
    SagaRegisterService sagaRegisterService;
    @Mock
    DistributedTaskService distributedTaskService;
    @Mock
    SagaManager sagaManager;
    @Mock
    SagaHelper sagaHelper;

    DistributionSagaService distributionSagaService;
    TaskDef<SagaPipeline> rootTaskDef;
    SagaPipeline sagaPipeline;

    static class TestSagaOperations {
        Integer sum(int input) {
            return input;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        distributionSagaService = new DistributionSagaServiceImpl(
            transactionManager,
            sagaResolver,
            sagaRegisterService,
            distributedTaskService,
            sagaManager,
            sagaHelper
        );

        rootTaskDef = TaskDef.privateTaskDef("root-saga-method", SagaPipeline.class);
        sagaPipeline = new SagaPipeline();
        sagaPipeline.addAction(SagaAction.builder()
            .sagaMethodTaskName("root-saga-method")
            .build());

        Method sumMethod = TestSagaOperations.class.getDeclaredMethod("sum", int.class);
        lenient().when(sagaResolver.resolveAsOperand(any()))
            .thenReturn(SagaOperand.builder()
                .method(sumMethod)
                .targetObject(new TestSagaOperations())
                .taskDef(rootTaskDef)
                .build()
            );
        lenient().when(sagaResolver.resolveByTaskName("root-saga-method")).thenReturn(rootTaskDef);
        lenient().when(sagaHelper.buildContextFor(any(), any(), any(), any(), any(), any())).thenReturn(sagaPipeline);
        lenient().when(distributedTaskService.schedule(any(), any())).thenReturn(TaskId.builder().build());
    }

    @SuppressWarnings("unchecked")
    private ExecutionContext<SagaPipeline> captureScheduledContext() throws Exception {
        var contextCaptor = ArgumentCaptor.forClass(ExecutionContext.class);
        verify(distributedTaskService).schedule(eq(rootTaskDef), contextCaptor.capture());
        return (ExecutionContext<SagaPipeline>) contextCaptor.getValue();
    }

    @Test
    void shouldAttachMetadataToRootTaskContext() throws Exception {
        //when
        var operations = new TestSagaOperations();
        var metadata = Metadata.of("traceId", "123");
        var settings = SagaSettings.builder().build();

        //do
        distributionSagaService.create("saga", settings, metadata)
            .registerToRun(operations::sum, 10)
            .start();

        //verify
        ExecutionContext<SagaPipeline> context = captureScheduledContext();
        assertThat(context.getMetadata().getSingle("traceId")).contains("123");
        assertThat(context.getInputMessageOpt()).contains(sagaPipeline);
    }

    @Test
    void shouldAttachMetadataToRootTaskContextWithAffinity() throws Exception {
        //when
        var operations = new TestSagaOperations();
        var metadata = Metadata.of("tenant", "acme");
        var settings = SagaSettings.builder().build();

        //do
        distributionSagaService.createWithAffinity("saga", "group", "affinity", settings, metadata)
            .registerToRun(operations::sum, 10)
            .start();

        //verify
        ExecutionContext<SagaPipeline> context = captureScheduledContext();
        assertThat(context.getMetadata().getSingle("tenant")).contains("acme");
        assertThat(context.getAffinityGroup()).isEqualTo("group");
        assertThat(context.getAffinity()).isEqualTo("affinity");
    }

    @Test
    void shouldUseRegisteredSagaSettingsWhenMetadataOverloadIsUsed() throws Exception {
        //when
        var operations = new TestSagaOperations();
        var settings = SagaSettings.builder().build();
        lenient().when(sagaRegisterService.getSagaSettings("saga")).thenReturn(settings);

        //do
        distributionSagaService.create("saga", Metadata.of("key", "value"))
            .registerToRun(operations::sum, 10)
            .start();

        //verify
        verify(sagaRegisterService).getSagaSettings("saga");
        ExecutionContext<SagaPipeline> context = captureScheduledContext();
        assertThat(context.getMetadata().getSingle("key")).contains("value");
    }

    @Test
    void shouldKeepMetadataForSagaStartedWithoutInput() throws Exception {
        //when
        var operations = new TestSagaOperations();
        var metadata = Metadata.of("traceId", "abc");
        var settings = SagaSettings.builder().build();

        //do: registerToConsume path -> SagaFlowBuilderWithoutInput.start()
        distributionSagaService.create("saga", settings, metadata)
            .registerToConsume(operations::sum, 10)
            .start();

        //verify
        ExecutionContext<SagaPipeline> context = captureScheduledContext();
        assertThat(context.getMetadata().getSingle("traceId")).contains("abc");
    }

    @Test
    void shouldCreateSagaWithoutMetadataByDefault() throws Exception {
        //when
        var operations = new TestSagaOperations();
        var settings = SagaSettings.builder().build();

        //do
        distributionSagaService.create("saga", settings)
            .registerToRun(operations::sum, 10)
            .start();

        //verify
        ExecutionContext<SagaPipeline> context = captureScheduledContext();
        assertThat(context.getMetadata()).isEqualTo(Metadata.empty());
    }
}
