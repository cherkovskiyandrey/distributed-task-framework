package com.distributed_task_framework.saga;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMapper;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMerger;
import com.distributed_task_framework.saga.mappers.SagaMethodPropertiesMapper;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.service.DistributedTaskService;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

//todo
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaConfigurationDiscoveryProcessor implements BeanPostProcessor {

    DistributedTaskService distributedTaskService;
    SagaTaskFactory sagaTaskFactory;
    SagaConfiguration sagaConfiguration;
    DistributedTaskProperties properties;
    DistributedTaskPropertiesMapper distributedTaskPropertiesMapper;
    DistributedTaskPropertiesMerger distributedTaskPropertiesMerger;
    SagaMethodPropertiesMapper sagaMethodPropertiesMapper;

    @SuppressWarnings("NullableProblems")
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
//        registerSagaMethodIfExists(bean);
//        registerSagaRevertMethodIfExists(bean);
        return bean;
    }

//    private void registerSagaMethodIfExists(Object bean) {
//        Arrays.stream(ReflectionUtils.getUniqueDeclaredMethods(AopUtils.getTargetClass(bean)))
//            .filter(method -> ReflectionHelper.findAnnotation(method, SagaMethod.class).isPresent())
//            .forEach(method -> {
//                @SuppressWarnings("OptionalGetWithoutIsPresent")
//                SagaMethod sagaMethodAnnotation = ReflectionHelper.findAnnotation(method, SagaMethod.class).get();
//                var taskName = SagaNamingUtils.taskNameFor(method);
//                if (methodToSagaOperation.containsKey(taskName)) {
//                    throw new SagaMethodDuplicateException(taskName);
//                }
//
//                var taskDef = TaskDef.privateTaskDef(taskName, SagaPipeline.class);
//                methodToSagaOperation.put(taskName, new SagaOperand(method, taskDef));
//                sagaMethodDiscovery.registerMethod(method.toString(), sagaMethodAnnotation);
//
//                SagaTask sagaTask = sagaTaskFactory.sagaTask(
//                    taskDef,
//                    method,
//                    bean,
//                    sagaMethodAnnotation
//                );
//                var taskSettings = buildTaskSettings(method);
//                distributedTaskService.registerTask(sagaTask, taskSettings);
//            });
//    }
//
//    private void registerSagaRevertMethodIfExists(Object bean) {
//        Arrays.stream(ReflectionUtils.getUniqueDeclaredMethods(AopUtils.getTargetClass(bean)))
//            .filter(method -> ReflectionHelper.findAnnotation(method, SagaRevertMethod.class).isPresent())
//            .forEach(method -> {
//                @SuppressWarnings("OptionalGetWithoutIsPresent")
//                SagaRevertMethod sagaRevertMethodAnnotation = ReflectionHelper.findAnnotation(method, SagaRevertMethod.class).get();
//                var revertTaskName = SagaNamingUtils.taskNameFor(method);
//                var taskDef = TaskDef.privateTaskDef(revertTaskName, SagaPipeline.class);
//                if (revertMethodToSagaOperation.containsKey(revertTaskName)) {
//                    throw new SagaMethodDuplicateException(revertTaskName);
//                }
//                revertMethodToSagaOperation.put(revertTaskName, new SagaOperand(method, taskDef));
//                sagaMethodDiscovery.registerMethod(method.toString(), sagaRevertMethodAnnotation);
//
//                SagaRevertTask sagaRevertTask = sagaTaskFactory.sagaRevertTask(
//                    taskDef,
//                    method,
//                    bean
//                );
//                var taskSettings = buildTaskSettings(method);
//                distributedTaskService.registerTask(sagaRevertTask, taskSettings);
//            });
//    }

//    private TaskSettings buildTaskSettings(Method method) {
//        var taskPropertiesGroup = Optional.ofNullable(properties.getTaskPropertiesGroup());
//        var sagaMethodPropertiesGroup = Optional.ofNullable(sagaConfiguration.getSagaMethodPropertiesGroup());
//
//        var dtfDefaultCodeTaskProperties = distributedTaskPropertiesMapper.map(TaskSettings.DEFAULT);
//        var sagaCustomCodeTaskProperties = fillCustomProperties(method);
//
//        var dtfDefaultConfTaskProperties = taskPropertiesGroup
//            .map(DistributedTaskProperties.TaskPropertiesGroup::getDefaultProperties)
//            .orElse(null);
//        var sagaDefaultConfTaskProperties = sagaMethodPropertiesGroup
//            .map(SagaConfiguration.SagaMethodPropertiesGroup::getDefaultSagaMethodProperties)
//            .map(sagaMethodPropertiesMapper::map)
//            .orElse(null);
//        var sagaCustomConfTaskProperties = sagaMethodPropertiesGroup
//            .map(SagaConfiguration.SagaMethodPropertiesGroup::getSagaMethodProperties)
//            .map(sagaMethodProperties -> sagaMethodProperties.get(SagaNamingUtils.sagaMethodNameFor(method)))
//            .map(sagaMethodPropertiesMapper::map)
//            .orElse(null);
//
//        var dtfDefaultTaskProperties = distributedTaskPropertiesMerger.merge(
//            dtfDefaultCodeTaskProperties,
//            dtfDefaultConfTaskProperties
//        );
//        var sagaDefaultTaskProperties = distributedTaskPropertiesMerger.merge(
//            dtfDefaultTaskProperties,
//            sagaDefaultConfTaskProperties
//        );
//
//        var sagaCustomTaskProperties = distributedTaskPropertiesMerger.merge(
//            sagaCustomCodeTaskProperties,
//            sagaCustomConfTaskProperties
//        );
//
//        var taskProperties = distributedTaskPropertiesMerger.merge(
//            sagaDefaultTaskProperties,
//            sagaCustomTaskProperties
//        );
//
//        return distributedTaskPropertiesMapper.map(taskProperties);
//    }
//
//    private DistributedTaskProperties.TaskProperties fillCustomProperties(Method method) {
//        var taskProperties = new DistributedTaskProperties.TaskProperties();
//        fillExecutionGuarantees(method, taskProperties);
//        return taskProperties;
//    }
//
//    private void fillExecutionGuarantees(Method method, DistributedTaskProperties.TaskProperties taskProperties) {
//        ReflectionHelper.findAnnotation(method, Transactional.class)
//            .ifPresent(executionGuarantees ->
//                taskProperties.setExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE.name())
//            );
//    }
}
