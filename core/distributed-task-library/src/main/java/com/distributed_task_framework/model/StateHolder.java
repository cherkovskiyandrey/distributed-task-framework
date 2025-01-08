package com.distributed_task_framework.model;

import jakarta.annotation.Nullable;
import lombok.AllArgsConstructor;

import java.util.Optional;

@AllArgsConstructor
public class StateHolder<U> {
    @Nullable
    private U state;

    public Optional<U> get() {
        return Optional.ofNullable(state);
    }

    public void set(U state) {
        this.state = state;
    }

    public boolean isEmpty() {
        return state == null;
    }
}
