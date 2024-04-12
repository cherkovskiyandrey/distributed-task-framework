package com.distributed_task_framework.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.springframework.lang.Nullable;
import com.distributed_task_framework.exception.InputMessageIsAbsentException;
import com.distributed_task_framework.utils.StringUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@ToString
@Getter
@FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@SuperBuilder(toBuilder = true)
@Jacksonized
public class ExecutionContext<T> {
    /**
     * Unique id in order to trace sequence of calling of tasks in the DAG.
     */
    UUID workflowId;
    /**
     * Date when current workflow instance denoted by {@link this#workflowId} has been created.
     */
    LocalDateTime workflowCreatedDateUtc;
    /**
     * Optional name of workflow.
     */
    @Nullable
    String workflowName;
    /**
     * Current task id.
     */
    TaskId currentTaskId;
    /**
     * Optional unique string represents data which current task is responsible for.
     * It is used in order to ask framework to run all tasks with the same affinity
     * sequentially, regardless if they can exist and can be invoked simultaneously.
     * <p>
     * For tasks included in one workflow which affinity is the same, framework guarantees
     * that none of parallel existed but later created workflows will be run in parallel.
     * It is fair in scope denoted by {@link this#getAffinityGroup()}
     */
    @Nullable
    String affinity;
    /**
     * Group of workflows where all workflows with the same {@link this#getAffinity()} run sequentially.
     */
    @Nullable
    String affinityGroup;

    /**
     * Input message for certain task.
     */
    @JsonProperty
    @Getter(AccessLevel.NONE)
    @Nullable
    T inputMessage;

    /**
     * Messages for join task.
     */
    @Builder.Default
    List<T> inputJoinTaskMessages = List.of();

    /**
     * Execution attempt of this task.
     */
    int executionAttempt;

    public static <T> ExecutionContext<T> empty() {
        return ExecutionContext.<T>builder()
                .workflowId(UUID.randomUUID())
                .inputMessage(null)
                .build();
    }

    /**
     * Create simple context with random workflowId and inputMessage.
     *
     * @param inputMessage
     * @param <T>
     * @return
     */
    public static <T> ExecutionContext<T> simple(T inputMessage) {
        return ExecutionContext.<T>builder()
                .workflowId(UUID.randomUUID())
                .inputMessage(Objects.requireNonNull(inputMessage))
                .build();
    }

    /**
     * Create context with inputMessage and affinity in affinityGroup and new workflowId.
     *
     * @param inputMessage
     * @param affinityGroup
     * @param affinity
     * @param <T>
     * @return
     */
    public static <T> ExecutionContext<T> withAffinityGroup(T inputMessage,
                                                            String affinityGroup,
                                                            String affinity) {
        return ExecutionContext.<T>builder()
                .workflowId(UUID.randomUUID())
                .affinityGroup(StringUtils.requireNotBlank(affinityGroup, "affinityGroup"))
                .affinity(StringUtils.requireNotBlank(affinity, "affinity"))
                .inputMessage(Objects.requireNonNull(inputMessage))
                .build();
    }

    /**
     * Reuse settings of current context in order to create new one with new message only.
     *
     * @param inputMessage
     * @return
     * @param <U>
     */
    public <U> ExecutionContext<U> withNewMessage(U inputMessage) {
        return ExecutionContext.<U>builder()
                .workflowId(this.getWorkflowId())
                .workflowCreatedDateUtc(this.getWorkflowCreatedDateUtc())
                .workflowName(this.getWorkflowName())
                .affinity(this.affinity)
                .affinityGroup(this.affinityGroup)
                .inputMessage(inputMessage)
                .build();
    }

    /**
     * Reuse settings of current context in order to create new one without message.
     *
     * @return
     * @param <U>
     */
    public <U> ExecutionContext<U> withEmptyMessage() {
        return ExecutionContext.<U>builder()
                .workflowId(this.getWorkflowId())
                .workflowCreatedDateUtc(this.getWorkflowCreatedDateUtc())
                .workflowName(this.getWorkflowName())
                .affinity(this.affinity)
                .affinityGroup(this.affinityGroup)
                .inputMessage(null)
                .build();
    }

    @JsonIgnore
    public Optional<T> getInputMessageOpt() {
        return Optional.ofNullable(inputMessage);
    }

    /**
     * Return inputMessage if exists
     * otherwise throw {@link InputMessageIsAbsentException}
     *
     * @return
     */
    @JsonIgnore
    public T getInputMessageOrThrow() throws InputMessageIsAbsentException {
        if (inputMessage == null) {
            throw new InputMessageIsAbsentException();
        }
        return inputMessage;
    }
}
