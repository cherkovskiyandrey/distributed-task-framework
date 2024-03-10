package com.distributed_task_framework.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.springframework.lang.Nullable;
import com.distributed_task_framework.persistence.entity.RemoteCommandEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.impl.local_commands.SaveCommand;
import com.distributed_task_framework.service.internal.LocalCommand;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.settings.TaskSettings;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@NotThreadSafe
@Getter
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Builder(toBuilder = true)
public class WorkerContext {
    public static final WorkerContext EMPTY = WorkerContext.builder().build();

    UUID workflowId;
    @Nullable
    String workflowName;
    TaskId currentTaskId;
    TaskSettings taskSettings;
    TaskEntity taskEntity;
    @Builder.Default
    List<LocalCommand> localCommands = Lists.newArrayList();
    @Builder.Default
    List<RemoteCommandEntity> remoteCommandsToSend = Lists.newArrayList();
    @Builder.Default
    Map<UUID, JoinTaskMessage<Object>> taskMessagesToSave = Maps.newHashMap();
    @Builder.Default
    Set<UUID> dropJoinTasksIds = Sets.newHashSet();

    @SuppressWarnings("unchecked")
    public <T> List<JoinTaskMessage<T>> getJoinMessagesFromBranch(TaskDef<T> taskDef) {
        return taskMessagesToSave.values().stream()
                .filter(message -> taskDef.getTaskName().equals(message.getTaskId().getTaskName()))
                .map(message -> (JoinTaskMessage<T>) message)
                .toList();
    }

    @SuppressWarnings("unchecked")
    public <T> void setJoinMessageToBranch(Collection<JoinTaskMessage<T>> joinTaskMessage) {
        joinTaskMessage.forEach(message -> taskMessagesToSave.put(
                message.getTaskId().getId(),
                (JoinTaskMessage<Object>) message)
        );
    }

    @SuppressWarnings("unchecked")
    public <T> void replaceJoinMessageToBranch(Collection<JoinTaskMessage<T>> joinTaskMessage) {
        joinTaskMessage.forEach(message -> {
                    UUID taskId = message.getTaskId().getId();
                    if (!taskMessagesToSave.containsKey(taskId)) {
                        throw new IllegalArgumentException("Task by=[%s] don't have a path to task=[%s]".formatted(
                                currentTaskId, taskId
                        ));
                    }
                    taskMessagesToSave.put(
                            message.getTaskId().getId(),
                            (JoinTaskMessage<Object>) message);
                }
        );
    }

    public boolean isProcessedByLocalCommand() {
        return localCommands.stream()
                .anyMatch(localCommand -> localCommand.hasTask(currentTaskId));
    }

    public boolean alreadyInTransaction() {
        return TaskSettings.ExecutionGuarantees.AT_LEAST_ONCE.equals(taskSettings.getExecutionGuarantees());
    }

    public boolean hasNewLocalChildren() {
        return localCommands.stream()
                .filter(localCommand -> !localCommand.hasTask(currentTaskId))
                .anyMatch(localCommand -> localCommand instanceof SaveCommand);
    }

    public Set<UUID> getAllChildrenIds() {
        return localCommands.stream()
                .filter(localCommand -> !localCommand.hasTask(currentTaskId))
                .filter(localCommand -> localCommand instanceof SaveCommand)
                .map(localCommand -> (SaveCommand) localCommand)
                .map(localCommand -> localCommand.getTaskEntity().getId())
                .collect(Collectors.toSet());
    }

    public boolean hasCronTasksToSave(Set<UUID> taskIds, TaskRegistryService taskRegistryService) {
        return localCommands.stream()
                .filter(localCommand -> !localCommand.hasTask(currentTaskId))
                .filter(localCommand -> localCommand instanceof SaveCommand)
                .map(localCommand -> (SaveCommand) localCommand)
                .filter(saveCommand -> taskIds.contains(saveCommand.getTaskEntity().getId()))
                .anyMatch(localCommand -> taskRegistryService.getRegisteredLocalTask(localCommand.getTaskEntity().getTaskName())
                        .map(regTask -> regTask.getTaskSettings().hasCron())
                        .orElse(false)
                );
    }

    public boolean isCurrentTaskCron() {
        return taskSettings.hasCron();
    }
}
