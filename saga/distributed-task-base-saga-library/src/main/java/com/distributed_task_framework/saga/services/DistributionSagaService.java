package com.distributed_task_framework.saga.services;


import com.distributed_task_framework.saga.settings.SagaSettings;

import java.util.UUID;

/**
 * Entry point to build saga.
 */
public interface DistributionSagaService extends SagaRegisterService {

    /**
     * Factory method to create new saga.
     * Isn't protected by affinity (saga isn't serializable by affinity).
     *
     * @param name arbitrary name to mark saga. Can be not unique.
     * @param sagaSettings settings for this particular saga
     * @return
     */
    SagaFlowEntryPoint create(String name, SagaSettings sagaSettings);

    /**
     * Factory method to create new saga.
     * Isn't protected by affinity (saga isn't serializable by affinity).
     * The difference with method {@link DistributionSagaService#create(String, SagaSettings)}
     * in settings, they will be used from {@link SagaRegisterService#registerSagaSettings(String, SagaSettings)}.
     *
     * @param name arbitrary name to mark saga. Can be not unique.
     * @return
     */
    SagaFlowEntryPoint create(String name);

    /**
     * Factory method to create new saga.
     * Protected by affinity (saga is serializable by affinityGroup + affinity).
     *
     * @param name arbitrary name to mark saga. Can be not unique.
     * @param affinityGroup
     * @param affinity
     * @param sagaSettings settings for this particular saga
     * @return
     */
    SagaFlowEntryPoint createWithAffinity(String name, String affinityGroup, String affinity, SagaSettings sagaSettings);

    /**
     * Factory method to create new saga.
     * Protected by affinity (saga is serializable by affinityGroup + affinity).
     * The difference with method {@link DistributionSagaService#create(String, SagaSettings)}
     * in settings, it will be used from {@link SagaRegisterService#registerSagaSettings(String, SagaSettings)}.
     *
     * @param name arbitrary name to mark saga. Can be not unique.
     * @param affinityGroup
     * @param affinity
     * @return
     */
    SagaFlowEntryPoint createWithAffinity(String name, String affinityGroup, String affinity);

    /**
     * Get flow by trackId if exists.
     *
     * @param trackId       trackId for saga flow
     * @param trackingClass class for output result
     * @param <OUTPUT>      output type
     * @return {@link SagaFlow} if exists, empty otherwise
     */
    <OUTPUT> SagaFlow<OUTPUT> getFlow(UUID trackId, Class<OUTPUT> trackingClass);

    /**
     * Get flow by trackId if exists.
     *
     * @param trackId trackId for saga flow
     * @return {@link SagaFlowWithoutResult} if exists, empty otherwise
     */
    SagaFlowWithoutResult getFlow(UUID trackId);
}
