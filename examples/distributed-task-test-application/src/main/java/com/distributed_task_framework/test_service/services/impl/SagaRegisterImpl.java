package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.test_service.annotations.SagaMethod;
import com.distributed_task_framework.test_service.exceptions.SagaException;
import com.distributed_task_framework.test_service.exceptions.SagaMethodDuplicateException;
import com.distributed_task_framework.test_service.exceptions.SagaMethodResolvingException;
import com.distributed_task_framework.test_service.exceptions.SagaTaskNotFoundException;
import com.distributed_task_framework.test_service.models.SagaContext;
import com.distributed_task_framework.test_service.services.SagaContextDiscovery;
import com.distributed_task_framework.test_service.services.SagaRegister;
import com.distributed_task_framework.test_service.utils.ReflectionHelper;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component //todo: BeanPostProcessor doesn't work if create bean from method!
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaRegisterImpl implements SagaRegister, BeanPostProcessor {
    private static final String TASK_PREFIX = "_____SAGA";
    private static final String TASK_NAME_DELIMITER = "_";

    Map<String, TaskDef<SagaContext>> methodToTask = Maps.newHashMap();
    DistributedTaskService distributedTaskService;
    SagaContextDiscovery sagaContextDiscovery;
    TaskSerializer taskSerializer;

    @Override
    public <IN, OUT> TaskDef<SagaContext> resolve(Function<IN, OUT> operation) {
        SagaMethod sagaMethod = sagaMethodByFunction(operation);
        return resolveByMethodRef(sagaMethod);
    }

    @Override
    public <IN> TaskDef<SagaContext> resolve(Consumer<IN> operation) {
        SagaMethod sagaMethod = sagaMethodByConsumer(operation);
        return resolveByMethodRef(sagaMethod);
    }

    @Override
    public <T, U, R> TaskDef<SagaContext> resolve(BiFunction<T, U, R> operation) {
        SagaMethod sagaMethod = sagaMethodByBiFunction(operation);
        return resolveByMethodRef(sagaMethod);
    }

    private TaskDef<SagaContext> resolveByMethodRef(SagaMethod sagaMethod) {
        var taskName = taskNameFor(sagaMethod);
        var operationTask = methodToTask.get(taskName);
        if (operationTask == null) {
            throw new SagaTaskNotFoundException(taskName);
        }
        return operationTask;
    }

    private <T, R> SagaMethod sagaMethodByFunction(Function<T, R> function) {
        return sagaMethodByRunnable(() -> function.apply(null), function);
    }

    private <T> SagaMethod sagaMethodByConsumer(Consumer<T> consumer) {
        return sagaMethodByRunnable(() -> consumer.accept(null), consumer);
    }

    private <T, U, R> SagaMethod sagaMethodByBiFunction(BiFunction<T, U, R> biFunction) {
        return sagaMethodByRunnable(() -> biFunction.apply(null, null), biFunction);
    }

    private SagaMethod sagaMethodByRunnable(Runnable runnable, Object methodRef) {
        sagaContextDiscovery.beginDetection();
        try {
            runnable.run();
            return sagaContextDiscovery.getSagaMethod();
        } catch (Exception exception) {
            throw new SagaMethodResolvingException(methodRef.toString());
        } finally {
            sagaContextDiscovery.completeDetection();
        }
    }

    private static String taskNameFor(SagaMethod sagaMethodAnnotation) {
        String name = sagaMethodAnnotation.name();
        String version = "" + sagaMethodAnnotation.version();
        String exceptions = Arrays.stream(sagaMethodAnnotation.noRetryFor())
                .map(Class::getCanonicalName)
                .sorted()
                .collect(Collectors.joining(", "));

        if (exceptions.isEmpty()) {
            return String.join(TASK_NAME_DELIMITER,
                    TASK_PREFIX,
                    name,
                    version
            );
        }

        return String.join(TASK_NAME_DELIMITER,
                TASK_PREFIX,
                name,
                version,
                UUID.nameUUIDFromBytes(exceptions.getBytes(StandardCharsets.UTF_8)).toString()
        );
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Arrays.stream(ReflectionUtils.getAllDeclaredMethods(bean.getClass()))
                .filter(method -> ReflectionHelper.findAnnotation(method, SagaMethod.class).isPresent())
                .forEach(method -> {
                    @SuppressWarnings("OptionalGetWithoutIsPresent")
                    SagaMethod sagaMethodAnnotation = ReflectionHelper.findAnnotation(method, SagaMethod.class).get();
                    var taskName = taskNameFor(sagaMethodAnnotation);
                    var taskDef = TaskDef.privateTaskDef(taskName, SagaContext.class);
                    if (methodToTask.containsKey(taskName)) {
                        throw new SagaMethodDuplicateException(taskName);
                    }
                    methodToTask.put(taskName, taskDef);
                    sagaContextDiscovery.registerMethod(method.toString(), sagaMethodAnnotation);

                    distributedTaskService.registerTask(
                            new Task<SagaContext>() {
                                @Override
                                public TaskDef<SagaContext> getDef() {
                                    return taskDef;
                                }

                                // (sagaRootContext.getTarget()), because it is valid only in node created the flow
                                @Override
                                public void execute(ExecutionContext<SagaContext> executionContext) throws Exception {
                                    SagaContext sagaRootContext = executionContext.getInputMessageOrThrow();

                                    byte[] rootArgument = sagaRootContext.getSerializedArg();
                                    byte[] parentArgument = executionContext.getInputJoinTaskMessages().stream()
                                            .map(SagaContext::getSerializedArg)
                                            .filter(Objects::nonNull)
                                            .findFirst()
                                            .orElse(null);
                                    byte[] singleArgument = ObjectUtils.firstNonNull(rootArgument, parentArgument);

                                    var argTotal = method.getParameters().length;
                                    Object result = switch (argTotal) {
                                        case 0 -> ReflectionUtils.invokeMethod(method, bean);
                                        case 1 -> ReflectionUtils.invokeMethod(
                                                method,
                                                bean,
                                                toMethodArgTypedObject(singleArgument, method.getParameters()[0])
                                        );
                                        case 2 -> ReflectionUtils.invokeMethod(
                                                method,
                                                bean,
                                                toMethodArgTypedObject(parentArgument, method.getParameters()[0]),
                                                toMethodArgTypedObject(rootArgument, method.getParameters()[1])
                                        );
                                        default -> throw new SagaException(
                                                "Unexpected number of arguments: expected < 3, but passed %s".formatted(argTotal)
                                        );
                                    };

                                    TaskDef<SagaContext> nextOperationTask = sagaRootContext.getNextOperationTaskDef();
                                    if (nextOperationTask == null) {
                                        return; //last task in sequence
                                    }

                                    var nextTaskJoinMessage = distributedTaskService.getJoinMessagesFromBranch(nextOperationTask)
                                            .get(0); //we get only next level task
                                    nextTaskJoinMessage = nextTaskJoinMessage.toBuilder()
                                            .message(SagaContext.builder()
                                                    .serializedArg(result != null ? taskSerializer.writeValue(result) : null)
                                                    .build()
                                            )
                                            .build();
                                    distributedTaskService.setJoinMessageToBranch(nextTaskJoinMessage);
                                }

                                @Override
                                public boolean onFailureWithResult(FailedExecutionContext<SagaContext> failedExecutionContext) {
                                    //todo: invoke current revertHandler + think about revert DAG

                                    Throwable throwable = failedExecutionContext.getError();
                                    boolean isNoRetryException = Arrays.stream(sagaMethodAnnotation.noRetryFor())
                                            .map(thrCls -> ExceptionUtils.throwableOfType(throwable, thrCls))
                                            .anyMatch(Objects::nonNull);

                                    if (isNoRetryException) {
                                        log.warn("onFailureWithResult(): isNoRetryException=[{}]", throwable.toString());
                                        return true;
                                    }

                                    //todo: log + logic
                                    return false;
                                }
                            }
                    );
                });

        return bean;
    }

    private Object toMethodArgTypedObject(@Nullable byte[] argument, Parameter parameter) throws IOException {
        if (argument == null) {
            return null;
        }
        JavaType javaType = TypeFactory.defaultInstance().constructType(parameter.getParameterizedType());
        return taskSerializer.readValue(argument, javaType);
    }
}
