package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.saga.exceptions.SagaOutBoundException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Getter
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class SagaEmbeddedPipelineContext {
    final UUID sagaId;
    final List<SagaEmbeddedActionContext> sagaEmbeddedActionContexts;
    int cursor;
    boolean forward;

    public SagaEmbeddedPipelineContext() {
        this.sagaId = UUID.randomUUID();
        this.sagaEmbeddedActionContexts = Lists.newArrayList();
        this.cursor = -1;
        this.forward = true;
    }

    /**
     * Set direction to forward and move cursor to BEFORE begin.
     */
    public void rewind() {
        forward = true;
        cursor = -1;
    }

    /**
     * Set direction to backward and move cursor to BEFORE the first valid revert operation.
     */
    public void rewindToRevertFromCurrentPosition() {
        forward = false;
        cursor = Math.min(cursor + 1, sagaEmbeddedActionContexts.size());
        for (; cursor > 0; cursor -= 1) {
            if (hasValidRevertOperation(cursor - 1)) {
                return;
            }
        }
    }

    /**
     * Check whether exist SagaContext on next position?
     * Works for both directions.
     *
     * @return
     */
    @JsonIgnore
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean hasNext() {
        if (forward) {
            return cursor + 1 < sagaEmbeddedActionContexts.size();
        }

        for (var revCursor = cursor; revCursor > 0; revCursor -= 1) {
            if (hasValidRevertOperation(revCursor - 1)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Move cursor to one position.
     * Works for both directions.
     */
    public void moveToNext() {
        if (forward) {
            cursor += 1;
            checkBorders();
            return;
        }

        while ((cursor -= 1) >= 0) {
            if (hasValidRevertOperation(cursor)) {
                return;
            }
        }
        checkBorders();
    }

    /**
     * Move cursor to the end and point to the last or first element.
     * Work for both directions.
     */
    @VisibleForTesting
    void moveToEnd() {
        if (sagaEmbeddedActionContexts.isEmpty()) {
            return;
        }

        if (forward) {
            cursor = sagaEmbeddedActionContexts.size() - 1;
        } else {
            cursor = 0;
        }
    }

    /**
     * Return SagaContext on current position.
     *
     * @return
     */
    @JsonIgnore
    public SagaEmbeddedActionContext getCurrentSagaContext() {
        checkBorders();
        return sagaEmbeddedActionContexts.get(cursor);
    }

    @JsonIgnore
    public SagaEmbeddedActionContext getRootSagaContext() {
        checkBorders();
        return sagaEmbeddedActionContexts.get(0);
    }

    /**
     * Get parent SagaContext if exists.
     * Always return parent from forward standpoint.
     *
     * @return
     */
    @JsonIgnore
    public Optional<SagaEmbeddedActionContext> getParentSagaContext() {
        checkBorders();
        if (cursor - 1 >= 0) {
            return Optional.ofNullable(sagaEmbeddedActionContexts.get(cursor - 1));
        }
        return Optional.empty();
    }

    /**
     * Add SagaContext to next position.
     * Always add to forward direction.
     *
     * @param sagaEmbeddedActionContext
     */
    @JsonIgnore
    public void addSagaContext(SagaEmbeddedActionContext sagaEmbeddedActionContext) {
        if (!forward) {
            throw new UnsupportedOperationException("Isn't supported");
        }
        sagaEmbeddedActionContexts.add(sagaEmbeddedActionContext);
    }

    /**
     * Set SagaContext on current position.
     *
     * @param currentSagaEmbeddedActionContext
     */
    @JsonIgnore
    public void setCurrentSagaContext(SagaEmbeddedActionContext currentSagaEmbeddedActionContext) {
        checkBorders();
        sagaEmbeddedActionContexts.set(cursor, currentSagaEmbeddedActionContext);
    }

    private void checkBorders() {
        if (sagaEmbeddedActionContexts.isEmpty() || cursor < 0 || cursor >= sagaEmbeddedActionContexts.size()) {
            throw new SagaOutBoundException(
                "checkBorders(): rawCursor=[%d], sagaContexts.size=[%d]".formatted(cursor, sagaEmbeddedActionContexts.size())
            );
        }
    }

    @JsonIgnore
    private boolean hasValidRevertOperation(int cursor) {
        var sagaContext = sagaEmbeddedActionContexts.get(cursor);
        return sagaContext != null && sagaContext.getSagaRevertMethodTaskName() != null;
    }
}
