package com.distributed_task_framework.test_service.models;

import com.distributed_task_framework.test_service.exceptions.SagaOutBoundException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class SagaPipelineContext {
    @Getter
    private final UUID id = UUID.randomUUID();
    private final List<SagaContext> sagaContexts = Lists.newArrayList();
    int cursor = -1;
    boolean forward = true;

    /**
     * Set direction to forward and move cursor to BEFORE begin.
     */
    public void rewind() {
        cursor = -1;
    }

    /**
     * Set direction to backward and move cursor to BEFORE the first valid revert operation.
     */
    public void rewindToRevert() {
        for (cursor = sagaContexts.size(); cursor > 0; cursor -= 1) {
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
            return cursor + 1 < sagaContexts.size();
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
     * Return SagaContext on current position.
     *
     * @return
     */
    public SagaContext getCurrentSagaContext() {
        checkBorders();
        return sagaContexts.get(cursor);
    }

    /**
     * Get parent SagaContext if exists.
     * Always return parent from forward standpoint.
     *
     * @return
     */
    public Optional<SagaContext> getParentSagaContext() {
        checkBorders();
        if (cursor - 1 >= 0) {
            return Optional.ofNullable(sagaContexts.get(cursor - 1));
        }
        return Optional.empty();
    }

    /**
     * Add SagaContext to next position.
     * Always add to forward direction.
     *
     * @param sagaContext
     */
    public void addSagaContext(SagaContext sagaContext) {
        if (!forward) {
            throw new UnsupportedOperationException("Isn't supported");
        }
        sagaContexts.add(sagaContext);
    }

    /**
     * Set SagaContext on current position.
     *
     * @param currentSagaContext
     */
    public void setCurrentSagaContext(SagaContext currentSagaContext) {
        checkBorders();
        sagaContexts.set(cursor, currentSagaContext);
    }

    private void checkBorders() {
        if (sagaContexts.isEmpty() || cursor < 0 || cursor >= sagaContexts.size()) {
            throw new SagaOutBoundException(
                    "checkBorders(): rawCursor=[%d], sagaContexts.size=[%d]".formatted(cursor, sagaContexts.size())
            );
        }
    }

    @JsonIgnore
    private boolean hasValidRevertOperation(int cursor) {
        var sagaContext = sagaContexts.get(cursor);
        return sagaContext != null && sagaContext.getSagaRevertMethodTaskDef() != null;
    }
}
