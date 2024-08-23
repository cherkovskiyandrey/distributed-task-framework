package com.distributed_task_framework.autoconfigure.mapper;

import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.Retry;
import com.distributed_task_framework.settings.TaskSettings;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;
import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;

import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Mapper(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
public interface DistributedTaskPropertiesMapper {

    TaskSettings map(DistributedTaskProperties.TaskProperties taskProperties);

    default TaskSettings merge(@MappingTarget TaskSettings defaultTaskProperties, DistributedTaskProperties.TaskProperties taskProperties) {
        DistributedTaskProperties.TaskProperties defaultTaskSettings = map(defaultTaskProperties);
        DistributedTaskProperties.TaskProperties mergedTaskProperties = merge(defaultTaskSettings, taskProperties);
        return map(mergedTaskProperties);
    }

    DistributedTaskProperties.TaskProperties map(TaskSettings taskSettings);

    default CommonSettings merge(@MappingTarget CommonSettings commonSettings,
                                 @Nullable DistributedTaskProperties.Common common) {
        if (common == null) {
            return commonSettings;
        }
        CommonSettings.CompletionSettings completionSettings = common.getCompletion() != null ?
                merge(commonSettings.getCompletionSettings().toBuilder().build(), common.getCompletion()) :
                commonSettings.getCompletionSettings();
        CommonSettings.RegistrySettings registrySettings = common.getRegistry() != null ?
                merge(commonSettings.getRegistrySettings().toBuilder().build(), common.getRegistry()) :
                commonSettings.getRegistrySettings();
        CommonSettings.PlannerSettings plannerSettings = common.getPlanner() != null ?
                merge(commonSettings.getPlannerSettings(), common.getPlanner()) :
                commonSettings.getPlannerSettings();
        CommonSettings.WorkerManagerSettings workerManagerSettings = common.getWorkerManager() != null ?
                merge(commonSettings.getWorkerManagerSettings(), common.getWorkerManager()) :
                commonSettings.getWorkerManagerSettings();
        //todo: test for these properties
        CommonSettings.StatSettings statSettings = common.getStatistics() != null ?
                merge(commonSettings.getStatSettings(), common.getStatistics()) :
                commonSettings.getStatSettings();
        CommonSettings.DeliveryManagerSettings deliveryManagerSettings = common.getDeliveryManager() != null ?
                merge(commonSettings.getDeliveryManagerSettings(), common.getDeliveryManager()) :
                commonSettings.getDeliveryManagerSettings();
        return mergeInternal(commonSettings, common).toBuilder()
                .completionSettings(completionSettings)
                .registrySettings(registrySettings)
                .plannerSettings(plannerSettings)
                .workerManagerSettings(workerManagerSettings)
                .statSettings(statSettings)
                .deliveryManagerSettings(deliveryManagerSettings)
                .build();
    }

    default CommonSettings.StatSettings merge(@MappingTarget CommonSettings.StatSettings defaultStatistics,
                                              DistributedTaskProperties.Statistics statistics) {
        DistributedTaskProperties.Statistics defaultPropertiesRegistry = map(defaultStatistics);
        DistributedTaskProperties.Statistics mergedRegistry = merge(defaultPropertiesRegistry, statistics);
        return map(mergedRegistry);
    }

    CommonSettings.StatSettings map(DistributedTaskProperties.Statistics mergedStatistics);

    DistributedTaskProperties.Statistics merge(@MappingTarget DistributedTaskProperties.Statistics defaultStatistics,
                                               DistributedTaskProperties.Statistics statistics);

    DistributedTaskProperties.Statistics map(CommonSettings.StatSettings defaultStatistics);

    default CommonSettings mergeInternal(@MappingTarget CommonSettings commonSettings,
                                         DistributedTaskProperties.Common common) {
        DistributedTaskProperties.Common defaultCommon = map(commonSettings);
        DistributedTaskProperties.Common mergedSettings = merge(defaultCommon, common);
        return map(mergedSettings);
    }

    DistributedTaskProperties.Common merge(@MappingTarget DistributedTaskProperties.Common defaultCommon,
                                           DistributedTaskProperties.Common common);

    @Mapping(target = "completionSettings", ignore = true)
    @Mapping(target = "registrySettings", ignore = true)
    @Mapping(target = "plannerSettings", ignore = true)
    @Mapping(target = "workerManagerSettings", ignore = true)
    @Mapping(target = "statSettings", ignore = true)
    @Mapping(target = "deliveryManagerSettings", ignore = true)
    CommonSettings map(DistributedTaskProperties.Common mergedSettings);

    @Mapping(target = "completion", ignore = true)
    @Mapping(target = "registry", ignore = true)
    @Mapping(target = "planner", ignore = true)
    @Mapping(target = "workerManager", ignore = true)
    @Mapping(target = "statistics", ignore = true)
    @Mapping(target = "deliveryManager", ignore = true)
    DistributedTaskProperties.Common map(CommonSettings commonSettings);

    /**
     * @noinspection UnstableApiUsage
     */
    default CommonSettings.DeliveryManagerSettings merge(@MappingTarget CommonSettings.DeliveryManagerSettings deliveryManagerSettings,
                                                         DistributedTaskProperties.DeliveryManager deliveryManager) {
        var mergedManagerSettings = mergeInternal(deliveryManagerSettings, deliveryManager);
        if (!deliveryManager.getManageDelay().isEmpty()) {
            ImmutableRangeMap<Integer, Integer> polingDelay = mapRangeDelayProperty(deliveryManager.getManageDelay());
            mergedManagerSettings = mergedManagerSettings.toBuilder()
                    .manageDelay(polingDelay)
                    .build();
        }
        mergedManagerSettings = mergedManagerSettings.toBuilder()
                .retry(merge(deliveryManagerSettings.getRetry(), deliveryManager.getRetry()))
                .build();
        return mergedManagerSettings;
    }

    default Retry merge(@MappingTarget Retry defaultRetrySettings, DistributedTaskProperties.Retry retry) {
        if (retry == null) {
            return defaultRetrySettings.toBuilder()
                    .build();
        }
        DistributedTaskProperties.Retry defaultRetryProperties = map(defaultRetrySettings);
        defaultRetryProperties = mergeInternal(defaultRetryProperties, retry);
        var defaultFixed = defaultRetryProperties.getFixed() != null ?
                defaultRetryProperties.getFixed() :
                DistributedTaskProperties.Fixed.builder().build();
        var defaultBackoff = defaultRetryProperties.getBackoff() != null ?
                defaultRetryProperties.getBackoff() :
                DistributedTaskProperties.Backoff.builder().build();

        if (retry.getFixed() != null) {
            defaultFixed = merge(defaultFixed, retry.getFixed());
        }
        if (retry.getBackoff() != null) {
            defaultBackoff = merge(defaultBackoff, retry.getBackoff());
        }
        defaultRetryProperties = defaultRetryProperties.toBuilder()
                .fixed(defaultFixed)
                .backoff(defaultBackoff)
                .build();
        return map(defaultRetryProperties);
    }

    Retry map(DistributedTaskProperties.Retry defaultRetryProperties);

    DistributedTaskProperties.Retry map(Retry defaultRetrySettings);

    DistributedTaskProperties.Retry mergeInternal(@MappingTarget DistributedTaskProperties.Retry defaultRetrySettings,
                                                  DistributedTaskProperties.Retry retry);

    default CommonSettings.DeliveryManagerSettings mergeInternal(@MappingTarget CommonSettings.DeliveryManagerSettings deliveryManagerSettings,
                                                                 DistributedTaskProperties.DeliveryManager deliveryManager) {
        DistributedTaskProperties.DeliveryManager defaultDeliveryManager = map(deliveryManagerSettings);
        DistributedTaskProperties.DeliveryManager mergedRegistry = merge(defaultDeliveryManager, deliveryManager);
        return map(mergedRegistry);
    }

    CommonSettings.DeliveryManagerSettings map(DistributedTaskProperties.DeliveryManager mergedRegistry);

    /**
     * @noinspection UnstableApiUsage
     */
    default DistributedTaskProperties.DeliveryManager map(CommonSettings.DeliveryManagerSettings deliveryManagerSettings) {
        var deliveryManager = mapInternal(deliveryManagerSettings);
        Map<Integer, Integer> manageDelay = Maps.newHashMap();
        deliveryManagerSettings.getManageDelay().asMapOfRanges()
                .forEach((range, limit) -> manageDelay.put(range.upperEndpoint(), limit));
        return deliveryManager.toBuilder()
                .manageDelay(manageDelay)
                .build();
    }

    @Mapping(target = "manageDelay", ignore = true)
    DistributedTaskProperties.DeliveryManager mapInternal(CommonSettings.DeliveryManagerSettings deliveryManagerSettings);

    @Mapping(target = "manageDelay", ignore = true)
    DistributedTaskProperties.DeliveryManager merge(@MappingTarget DistributedTaskProperties.DeliveryManager deliveryManagerSettings,
                                                    DistributedTaskProperties.DeliveryManager workerManager);


    /**
     * @noinspection UnstableApiUsage
     */
    default CommonSettings.WorkerManagerSettings merge(@MappingTarget CommonSettings.WorkerManagerSettings workerManagerSettings,
                                                       DistributedTaskProperties.WorkerManager workerManager) {
        var mergedManagerSettings = mergeInternal(workerManagerSettings, workerManager);
        if (!workerManager.getManageDelay().isEmpty()) {
            ImmutableRangeMap<Integer, Integer> manageDelay = mapRangeDelayProperty(workerManager.getManageDelay());
            mergedManagerSettings = mergedManagerSettings.toBuilder()
                    .manageDelay(manageDelay)
                    .build();
        }
        return mergedManagerSettings;
    }

    default CommonSettings.WorkerManagerSettings mergeInternal(@MappingTarget CommonSettings.WorkerManagerSettings workerManagerSettings,
                                                               DistributedTaskProperties.WorkerManager workerManager) {
        DistributedTaskProperties.WorkerManager defaultWorkerManager = map(workerManagerSettings);
        DistributedTaskProperties.WorkerManager mergedRegistry = merge(defaultWorkerManager, workerManager);
        return map(mergedRegistry);
    }

    CommonSettings.WorkerManagerSettings map(DistributedTaskProperties.WorkerManager mergedRegistry);

    @Mapping(target = "manageDelay", ignore = true)
    DistributedTaskProperties.WorkerManager merge(@MappingTarget DistributedTaskProperties.WorkerManager defaultWorkerManager,
                                                  DistributedTaskProperties.WorkerManager workerManager);

    /**
     * @noinspection UnstableApiUsage
     */
    default DistributedTaskProperties.WorkerManager map(CommonSettings.WorkerManagerSettings workerManagerSettings) {
        var workerManager = mapInternal(workerManagerSettings);
        Map<Integer, Integer> manageDelay = Maps.newHashMap();
        workerManagerSettings.getManageDelay().asMapOfRanges()
                .forEach((range, limit) -> manageDelay.put(range.upperEndpoint(), limit));
        return workerManager.toBuilder()
                .manageDelay(manageDelay)
                .build();
    }

    @Mapping(target = "manageDelay", ignore = true)
    DistributedTaskProperties.WorkerManager mapInternal(CommonSettings.WorkerManagerSettings workerManagerSettings);

    default CommonSettings.CompletionSettings merge(@MappingTarget CommonSettings.CompletionSettings defaultCompletionSettings,
                                                    DistributedTaskProperties.Completion completion) {
        DistributedTaskProperties.Completion defualtPropertiesCompletion = map(defaultCompletionSettings);
        DistributedTaskProperties.Completion mergedCompletion = merge(defualtPropertiesCompletion, completion);
        return map(mergedCompletion);
    }

    CommonSettings.CompletionSettings map(DistributedTaskProperties.Completion mergedCompletion);

    DistributedTaskProperties.Completion merge(@MappingTarget DistributedTaskProperties.Completion defualtPropertiesCompletion,
                                               DistributedTaskProperties.Completion completion);

    DistributedTaskProperties.Completion map(CommonSettings.CompletionSettings defaultCompletionSettings);


    default CommonSettings.RegistrySettings merge(@MappingTarget CommonSettings.RegistrySettings defaultRegistrySettings,
                                                  DistributedTaskProperties.Registry registry) {
        DistributedTaskProperties.Registry defaultPropertiesRegistry = map(defaultRegistrySettings);
        DistributedTaskProperties.Registry mergedRegistry = merge(defaultPropertiesRegistry, registry);
        return map(mergedRegistry);
    }

    CommonSettings.RegistrySettings map(DistributedTaskProperties.Registry mergedRegistry);

    DistributedTaskProperties.Registry merge(@MappingTarget DistributedTaskProperties.Registry defaultPropertiesRegistry,
                                             DistributedTaskProperties.Registry registry);

    DistributedTaskProperties.Registry map(CommonSettings.RegistrySettings defaultRegistrySettings);

    /**
     * @noinspection UnstableApiUsage
     */
    default CommonSettings.PlannerSettings merge(@MappingTarget CommonSettings.PlannerSettings defaultPlannerSettings,
                                                 DistributedTaskProperties.Planner planner) {
        defaultPlannerSettings = mergeInternal(defaultPlannerSettings, planner);
        var plannerBuilder = defaultPlannerSettings.toBuilder();
        if (!planner.getPollingDelay().isEmpty()) {
            ImmutableRangeMap<Integer, Integer> pollingDelay = mapRangeDelayProperty(planner.getPollingDelay());
            plannerBuilder.pollingDelay(pollingDelay);
        }
        return plannerBuilder.build();
    }

    /**
     * @noinspection UnstableApiUsage
     */
    default ImmutableRangeMap<Integer, Integer> mapRangeDelayProperty(Map<Integer, Integer> rangeDelay) {
        List<Integer> orderedNumbers = rangeDelay.keySet().stream()
                .sorted()
                .toList();
        var rangeMapBuilder = ImmutableRangeMap.<Integer, Integer>builder();
        int lastNumber = -1;
        for (int number : orderedNumbers) {
            if (number <= lastNumber) {
                throw new TaskConfigurationException("Incorrect ordering of polling-delay");
            }
            rangeMapBuilder.put(Range.openClosed(lastNumber, number), rangeDelay.get(number));
            lastNumber = number;
        }
        return rangeMapBuilder.build();
    }

    /**
     * @noinspection UnstableApiUsage
     */
    default DistributedTaskProperties.Planner map(CommonSettings.PlannerSettings defaultPlannerSettings) {
        DistributedTaskProperties.Planner result = mapInternal(defaultPlannerSettings);
        Map<Integer, Integer> pollingDelay = Maps.newHashMap();
        defaultPlannerSettings.getPollingDelay().asMapOfRanges().forEach((range, limit) -> {
            pollingDelay.put(range.upperEndpoint(), limit);
        });
        return result.toBuilder()
                .pollingDelay(pollingDelay)
                .build();
    }

    @Mapping(target = "pollingDelay", ignore = true)
    DistributedTaskProperties.Planner mapInternal(CommonSettings.PlannerSettings defaultPlannerSettings);

    default CommonSettings.PlannerSettings mergeInternal(@MappingTarget CommonSettings.PlannerSettings defaultPlannerSettings,
                                                         DistributedTaskProperties.Planner planner) {
        DistributedTaskProperties.Planner defaultPropertiesPlanner = map(defaultPlannerSettings);
        DistributedTaskProperties.Planner mergedPlanner = merge(defaultPropertiesPlanner, planner);
        return map(mergedPlanner);
    }

    @Mapping(target = "pollingDelay", ignore = true)
    CommonSettings.PlannerSettings map(DistributedTaskProperties.Planner mergedPlanner);

    DistributedTaskProperties.Planner merge(@MappingTarget DistributedTaskProperties.Planner defaultPropertiesPlanner,
                                            DistributedTaskProperties.Planner planner);

    default DistributedTaskProperties.TaskProperties merge(@MappingTarget DistributedTaskProperties.TaskProperties defaultSettings,
                                                           DistributedTaskProperties.TaskProperties taskProperties) {
        DistributedTaskProperties.Retry defaultRetry = defaultSettings.getRetry() != null ? defaultSettings.getRetry() :
                DistributedTaskProperties.Retry.builder().build();
        DistributedTaskProperties.Backoff defaultBackoff = defaultSettings.getRetry() != null && defaultSettings.getRetry().getBackoff() != null ?
                defaultSettings.getRetry().getBackoff().toBuilder().build() :
                DistributedTaskProperties.Backoff.builder().build();

        DistributedTaskProperties.Fixed defaultFixed = defaultSettings.getRetry() != null && defaultSettings.getRetry().getFixed() != null ?
                defaultSettings.getRetry().getFixed().toBuilder().build() :
                DistributedTaskProperties.Fixed.builder().build();

        var result = mergeInternal(defaultSettings, taskProperties);
        if (taskProperties.getRetry() != null) {
            defaultRetry = mergeInternal(defaultRetry, taskProperties.getRetry());
        }
        if (taskProperties.getRetry() != null && taskProperties.getRetry().getBackoff() != null) {
            defaultBackoff = merge(defaultBackoff, taskProperties.getRetry().getBackoff());
        }
        if (taskProperties.getRetry() != null && taskProperties.getRetry().getFixed() != null) {
            defaultFixed = merge(defaultFixed, taskProperties.getRetry().getFixed());
        }
        return result.toBuilder()
                .retry(defaultRetry.toBuilder()
                        .backoff(defaultBackoff)
                        .fixed(defaultFixed)
                        .build())
                .build();
    }

    @Mapping(target = "retry", ignore = true)
    DistributedTaskProperties.TaskProperties mergeInternal(@MappingTarget DistributedTaskProperties.TaskProperties defaultSettings,
                                                           DistributedTaskProperties.TaskProperties taskProperties);

    DistributedTaskProperties.Backoff merge(@MappingTarget DistributedTaskProperties.Backoff defaultBackoff,
                                            DistributedTaskProperties.Backoff backoff);

    DistributedTaskProperties.Fixed merge(@MappingTarget DistributedTaskProperties.Fixed defaultBackoff,
                                          DistributedTaskProperties.Fixed backoff);
}
