package com.distributed_task_framework.saga.autoconfigure.annotations;

/**
 * Interface to handle cases when there are more than one bean of one type.
 */
public interface SagaSpecific {

    /**
     * Use this suffix to distinguish several beans with same type.
     * Important in case when these beans are involved to process any methods with {@link SagaMethod}
     *
     * @return prefix to add dtf task along with {@link SagaMethod}
     */
    String suffix();
}
