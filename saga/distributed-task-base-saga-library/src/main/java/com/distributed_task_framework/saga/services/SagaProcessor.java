package com.distributed_task_framework.saga.services;


import java.util.UUID;

public interface SagaProcessor {

    /**
     * Factory method to create new saga.
     * Isn't protected by affinity (saga isn't serializable by affinity).
     *
     * @param name arbitrary name to mark saga. Can be not unique.
     * @return
     */
    SagaEntryPoint create(String name);

    /**
     * Factory method to create new saga.
     * Protected by affinity (saga is serializable by affinityGroup + affinity).
     *
     * @param name arbitrary name to mark saga. Can be not unique.
     * @param affinityGroup
     * @param affinity
     * @return
     */
    SagaEntryPoint createWithAffinity(String name, String affinityGroup, String affinity);


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
