package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.test_service.annotations.SagaMethod;
import com.distributed_task_framework.test_service.annotations.SagaRevertMethod;
import com.distributed_task_framework.test_service.exceptions.SagaMethodDuplicateException;
import com.distributed_task_framework.test_service.exceptions.SagaMethodResolvingException;
import com.distributed_task_framework.test_service.exceptions.SagaTaskNotFoundException;
import com.distributed_task_framework.test_service.models.SagaContext;
import com.distributed_task_framework.test_service.models.SagaRevertContext;
import com.distributed_task_framework.test_service.services.SagaContextDiscovery;
import com.distributed_task_framework.test_service.services.SagaRegister;
import com.distributed_task_framework.test_service.utils.ReflectionHelper;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
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
    private static final String TASK_REVERT_PREFIX = "_____SAGA_REVERT";
    private static final String TASK_NAME_DELIMITER = "_";

    Map<String, TaskDef<SagaContext>> methodToTask = Maps.newHashMap();
    Map<String, TaskDef<SagaRevertContext>> revertMethodToTask = Maps.newHashMap();
    Map<TaskDef<SagaRevertContext>, Method> revertTaskToMethod = Maps.newHashMap();
    DistributedTaskService distributedTaskService;
    SagaContextDiscovery sagaContextDiscovery;
    TaskSerializer taskSerializer;
    SagaHelper sagaHelper;

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


    private static String revertTaskNameFor(SagaRevertMethod sagaRevertMethodAnnotation) {
        String name = sagaRevertMethodAnnotation.name();
        String version = "" + sagaRevertMethodAnnotation.version();

        return String.join(TASK_NAME_DELIMITER,
                TASK_REVERT_PREFIX,
                name,
                version
        );
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        registerSagaMethodIfExists(bean);
        registerSagaRevertMethodIfExists(bean);

        return bean;
    }

    private void registerSagaMethodIfExists(Object bean) {
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

                    //todo: consider creating of prototype bean
                    SagaTask sagaTask = SagaTask.builder()
                            .distributedTaskService(distributedTaskService)
                            .taskSerializer(taskSerializer)
                            .sagaHelper(sagaHelper)
                            .revertTaskToMethod(revertTaskToMethod)
                            .taskDef(taskDef)
                            .method(method)
                            .bean(bean)
                            .sagaMethodAnnotation(sagaMethodAnnotation)
                            .build();
                    distributedTaskService.registerTask(sagaTask);
                });
    }

    private void registerSagaRevertMethodIfExists(Object bean) {
        Arrays.stream(ReflectionUtils.getAllDeclaredMethods(bean.getClass()))
                .filter(method -> ReflectionHelper.findAnnotation(method, SagaRevertMethod.class).isPresent())
                .forEach(method -> {
                    @SuppressWarnings("OptionalGetWithoutIsPresent")
                    SagaRevertMethod sagaRevertMethodAnnotation = ReflectionHelper.findAnnotation(method, SagaRevertMethod.class).get();
                    var revertTaskName = revertTaskNameFor(sagaRevertMethodAnnotation);
                    var taskDef = TaskDef.privateTaskDef(revertTaskName, SagaRevertContext.class);
                    if (revertMethodToTask.containsKey(revertTaskName)) {
                        throw new SagaMethodDuplicateException(revertTaskName);
                    }
                    revertMethodToTask.put(revertTaskName, taskDef);
                    sagaContextDiscovery.registerMethod(method.toString(), sagaRevertMethodAnnotation);

                    //todo: consider creating of prototype bean
                    SagaRevertTask sagaRevertTask = SagaRevertTask.builder()
                            .sagaHelper(sagaHelper)
                            .taskDef(taskDef)
                            .method(method)
                            .bean(bean)
                            .build();
                    distributedTaskService.registerTask(sagaRevertTask);
                });
    }
}
