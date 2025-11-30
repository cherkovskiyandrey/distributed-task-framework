package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.settings.SagaSettings;

public interface SagaRegisterSettingsService {
    /**
     * Register saga settings by name.
     *
     * @param name
     * @param sagaSettings
     */
    void registerSagaSettings(String name, SagaSettings sagaSettings);

    /**
     * Register default saga settings, which will be returned for unregistered sagas.
     *
     * @param sagaSettings
     */
    void registerDefaultSagaSettings(SagaSettings sagaSettings);

    /**
     * Return saga settings. If settings was registered - return it,
     * otherwise if default saga settings was registered - return it,
     * otherwise return {@link SagaSettings#DEFAULT}
     *
     * @param name
     * @return
     */
    SagaSettings getSagaSettings(String name);

    /**
     * Unregister saga settings by name.
     *
     * @param name
     */
    void unregisterSagaSettings(String name);
}
