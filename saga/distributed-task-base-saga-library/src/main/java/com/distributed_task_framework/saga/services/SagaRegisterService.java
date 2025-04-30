package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;

import java.lang.reflect.Method;
import java.util.Collection;

public interface SagaRegisterService {

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

    /**
     * Register new saga method on the provided method in the object with settings.
     *
     * @param name               the unique name of saga method
     * @param method             the method has to be assigned to new saga method
     * @param object             the object where sagaMethod is located
     * @param proxyWrappers      the proxy wrappers over provided object if presents
     * @param sagaMethodSettings the settings of new saga method
     */
    void registerSagaMethod(String name,
                            Method method,
                            Object object,
                            Collection<Object> proxyWrappers,
                            SagaMethodSettings sagaMethodSettings);

    /**
     * Register new saga method on the provided method reference in the object with settings.
     *
     * @param name               the unique name of saga method
     * @param methodRef          the method has to be assigned to new saga method
     * @param object             the object where sagaMethod is located
     * @param proxyWrappers      the proxy wrappers over provided object if presents     *
     * @param sagaMethodSettings the settings of new saga method
     * @param <T>                input type
     * @param <R>                return type
     */
    <T, R> void registerSagaMethod(String name,
                                   SagaFunction<T, R> methodRef,
                                   Object object,
                                   Collection<Object> proxyWrappers,
                                   SagaMethodSettings sagaMethodSettings);

    /**
     * Register new saga revert method on the provided method in the object with settings.
     *
     * @param name               the unique name of saga method
     * @param method             the method has to be assigned to new saga method
     * @param object             the object where sagaMethod is located
     * @param proxyWrappers      the proxy wrappers over provided object if presents
     * @param sagaMethodSettings the settings of new saga method
     */
    void registerSagaRevertMethod(String name,
                                  Method method,
                                  Object object,
                                  Collection<Object> proxyWrappers,
                                  SagaMethodSettings sagaMethodSettings);

    /**
     * Unregister saga method.
     *
     * @param name the unique name of saga method
     */
    void unregisterSagaMethod(String name);
}
