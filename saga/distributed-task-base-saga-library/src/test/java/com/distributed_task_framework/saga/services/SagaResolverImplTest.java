package com.distributed_task_framework.saga.services;

import org.junit.jupiter.api.Test;

//todo:
class SagaResolverImplTest {

    @Test
    void registerOperand() {
    }

    @Test
    void unregisterOperand() {
    }

    @Test
    void resolveAsOperand() {
    }

    @Test
    void resolveAsMethod() {
        // todo: case when inherited
        // 1. A(foo(), boo()) <-- B(override only boo())
        // lambda = B::foo -> must right resolved as A::foo
    }

    @Test
    void resolveByTaskName() {
    }
}