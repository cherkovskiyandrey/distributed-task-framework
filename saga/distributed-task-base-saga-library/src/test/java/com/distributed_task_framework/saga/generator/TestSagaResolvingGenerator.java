package com.distributed_task_framework.saga.generator;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.utils.TestUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.List;

import static org.mockito.Mockito.spy;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TestSagaResolvingGenerator {
    SagaResolver sagaResolver;

    public <T> TestSagaResolvingModel<T> generateAndReg(TestSagaResolvingModelSpec spec) {
        return generate(spec.toBuilder().register(true).build());
    }

    @SuppressWarnings("unchecked")
    public <T> TestSagaResolvingModel<T> generate(TestSagaResolvingModelSpec spec) {
        var proxyList = spec.isWithProxy() ? List.of(spy(spec.getObject())) : List.of();
        var method = spec.getReturnType() != null ?
            TestUtils.findMethod(
                spec.getLookupInClass() != null ? spec.getLookupInClass() : spec.getObject().getClass(),
                spec.getMethodName(),
                spec.getReturnType(),
                spec.getParameters()
            ) :
            TestUtils.findFirstMethod(spec.getObject().getClass(), spec.getMethodName());

        var taskDef = TaskDef.privateTaskDef(RandomStringUtils.random(10), SagaPipeline.class);
        var operand = SagaOperand.builder()
            .taskDef(taskDef)
            .targetObject(spec.getObject())
            .proxyWrappers(proxyList)
            .method(method)
            .build();
        var operandName = RandomStringUtils.random(10);
        if (spec.isRegister()) {
            sagaResolver.registerOperand(operandName, operand);
        }

        return TestSagaResolvingModel.<T>builder()
            .targetObject((T) spec.getObject())
            .proxy((T) proxyList.stream().findFirst().orElse(null))
            .taskDef(taskDef)
            .operandName(operandName)
            .sagaOperand(operand)
            .method(method)
            .build();
    }

}
