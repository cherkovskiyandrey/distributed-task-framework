package com.distributed_task_framework.saga.autoconfigure.annotations;

/**
 * Interface to handle cases when there are more than one bean of one type.
 */
public interface SagaSpecific {

    /**
     * Use this suffix to distinguish two beans with same type.
     * Important in case when these two beans are involved to process any methods with {@link SagaMethod}
     *
     * @return prefix to add dtf task along with {@link SagaMethod}
     */
    String suffix();

    /**
     * Use to mark particular bean to ignore all direct and extended methods with {@link SagaMethod}
     * Useful in case when there is only one bean responsible to handle methods with {@link SagaMethod}
     * where other beans with same type aren't.
     *
     * @return
     */
    boolean ignore();
}
