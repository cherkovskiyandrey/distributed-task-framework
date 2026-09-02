package com.distributed_task_framework.utils;

/**
 * Interface to place all logic connected with
 * init, start, stop and cleanup for any service/bean in dtf and extended libraries.</br>
 * </br>
 * Framework which is responsible to create an application context
 * have to provide guarantees that method {@link DistributedTaskServiceLifecycle#start()}
 * will be invoked after all application context is created.
 * And is responsible to provide guarantees that method {@link DistributedTaskServiceLifecycle#stop()}
 * will be invoked before application destroy context.</br>
 * </br>
 * Also, framework have to provide guarantees about ordering:
 * <ol>
 *     <li>ordering for {@link DistributedTaskServiceLifecycle#init()} = ordering for create bean/service</li>
 *     <li>ordering for {@link DistributedTaskServiceLifecycle#start()} = ordering for start bean/service</li>
 *     <li>ordering for {@link DistributedTaskServiceLifecycle#stop()} = reverse order for start bean/service</li>
 *     <li>ordering for {@link DistributedTaskServiceLifecycle#cleanup()} = reverse order for create bean/service</li>
 *     <li>invoke all {@link DistributedTaskServiceLifecycle#init()} before invoking any {@link DistributedTaskServiceLifecycle#start()}</li>
 *     <li>invoke all {@link DistributedTaskServiceLifecycle#stop()} before invoking any {@link DistributedTaskServiceLifecycle#cleanup()}</li>
 * </ol>
 */
public interface DistributedTaskServiceLifecycle {

    /**
     * Is invoked during service is creating to initialize it.</br>
     * </br>
     * Warning: pay attention when use other services from this method, because they can't be initialized yet.
     * Only simple logic before application is started.
     * </br>
     *
     * @throws Exception
     */
    default void init() throws Exception {
    }

    /**
     * Is invoked after application context is fully initialized but before started.</br>
     * </br>
     * Place here post initialization logic.
     * Like starting background threads.
     * Or do configuration action on low level api from high level
     * which dangerous to do in initialization phase by restriction of current
     * initialization framework (for example, Spring PostConstruct, because context hasn't been created yet on this phase).</br>
     * </br>
     * Throwing exception from this method should lead to stop application.
     */
    default void start() throws Exception {
    }

    /**
     * Is invoked before application context is stoped.</br>
     * </br>
     * Place here pre-destroy logic.
     * Like stop background threads.</br>
     * On this phase application context hasn't been destroyed yet.</br>
     * </br>
     * Throwing exception from this method should not prevent to gracefully shutdown (invoking stop method for other services).
     */
    default void stop() throws Exception {
    }

    /**
     * Place logic to clean data in any storage. In this phase it is safe, because this phase is run
     * after {@link DistributedTaskServiceLifecycle#stop()} for all services.</br>
     * </br>
     * Throwing exception from this method should not prevent to gracefully shutdown (invoking cleanup method for other services).
     */
    default void cleanup() throws Exception {
    }
}
