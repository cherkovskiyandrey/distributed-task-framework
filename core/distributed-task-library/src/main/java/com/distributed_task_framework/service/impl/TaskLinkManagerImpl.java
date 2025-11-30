package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.JoinTaskExecution;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.JoinTaskMessageContainer;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskIdEntity;
import com.distributed_task_framework.persistence.entity.TaskLinkEntity;
import com.distributed_task_framework.persistence.entity.TaskMessageEntity;
import com.distributed_task_framework.persistence.entity.UUIDEntity;
import com.distributed_task_framework.persistence.repository.TaskLinkRepository;
import com.distributed_task_framework.persistence.repository.TaskMessageRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.TaskLinkManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.utils.JdbcTools;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TaskLinkManagerImpl implements TaskLinkManager {
    TaskRepository taskRepository;
    TaskLinkRepository taskLinkRepository;
    TaskMessageRepository taskMessageRepository;
    CommonSettings commonSettings;
    TaskSerializer taskSerializer;
    TaskMapper taskMapper;

    @Override
    public void createLinks(TaskId joinTaskId, List<TaskId> joinList) {
        List<TaskLinkEntity> joinTaskLinkEntities = joinList.stream()
            .map(taskId -> TaskLinkEntity.builder()
                .joinTaskId(joinTaskId.getId())
                .taskToJoinId(taskId.getId())
                .joinTaskName(joinTaskId.getTaskName())
                .build()
            )
            .collect(Collectors.toList());
        taskLinkRepository.saveAll(joinTaskLinkEntities);
    }

    @Override
    public void inheritLinks(UUID parent, Set<UUID> children) {
        List<TaskLinkEntity> parentJoinTaskLinkEntities = taskLinkRepository.findAllByTaskToJoinId(parent);
        Set<TaskLinkEntity> inheritedLinks = children.stream()
            .flatMap(child -> parentJoinTaskLinkEntities.stream()
                .map(entity -> entity.toBuilder()
                    .id(null) //new one
                    .taskToJoinId(child)
                    .completed(false)
                    .build()))
            .collect(Collectors.toSet());
        taskLinkRepository.saveAll(inheritedLinks);
    }

    @Override
    public void removeLinks(UUID currentTaskId) {
        taskLinkRepository.deleteAllByTaskToJoinId(currentTaskId);
    }

    @Override
    public Set<UUID> detectLeaves(Set<UUID> trees) {
        Set<UUID> intermediateTaskIds = taskLinkRepository.filterIntermediateTasks(JdbcTools.UUIDsToStringArray(trees))
            .stream()
            .map(UUIDEntity::getUuid)
            .collect(Collectors.toSet());
        return Sets.newHashSet(Sets.difference(trees, intermediateTaskIds));
    }

    @Override
    public <T> Collection<JoinTaskMessage<T>> getJoinMessages(TaskId currentTaskId, TaskDef<T> joinTaskDef) throws IOException {
        String appName = StringUtils.isNotBlank(joinTaskDef.getAppName()) ?
            joinTaskDef.getAppName() :
            commonSettings.getAppName();
        String joinTaskName = joinTaskDef.getTaskName();
        List<UUIDEntity> allJoinTasksFromBranch = taskLinkRepository.findAllJoinTasksFromBranchByName(
            currentTaskId.getId(),
            joinTaskName
        );
        Set<UUID> allAvailableJoinTasks = allJoinTasksFromBranch.stream()
            .map(UUIDEntity::getUuid)
            .collect(Collectors.toSet());
        if (allAvailableJoinTasks.isEmpty()) {
            throw new IllegalArgumentException("Task by=[%s] don't have a path to task=[%s]".formatted(
                currentTaskId, joinTaskName
            ));
        }

        var joinTaskIdToTaskIdEntities = taskRepository.findAllTaskId(allAvailableJoinTasks).stream()
            .collect(Collectors.toMap(TaskIdEntity::getId, Function.identity()));
        List<TaskMessageEntity> joinMessagesForBranch = taskMessageRepository.findAllByTaskToJoinIdAndJoinTaskIdIn(
            currentTaskId.getId(),
            allAvailableJoinTasks
        );
        Collection<JoinTaskMessage<T>> existedJoinTaskMessages = readMessage(
            joinMessagesForBranch,
            joinTaskIdToTaskIdEntities,
            joinTaskDef.getInputMessageType()
        );

        Set<UUID> existedJoinTaskIds = joinMessagesForBranch.stream()
            .map(TaskMessageEntity::getJoinTaskId)
            .collect(Collectors.toSet());

        Set<UUID> newJoinTaskMessageIds = Sets.newHashSet(Sets.difference(allAvailableJoinTasks, existedJoinTaskIds));
        List<JoinTaskMessage<T>> newJoinTaskMessages = newJoinTaskMessageIds.stream()
            .map(joinTaskId -> JoinTaskMessage.<T>builder()
                .taskId(taskMapper.map(joinTaskIdToTaskIdEntities.get(joinTaskId), appName))
                .build()
            ).toList();

        return ImmutableList.<JoinTaskMessage<T>>builder()
            .addAll(existedJoinTaskMessages)
            .addAll(newJoinTaskMessages)
            .build();
    }

    @Override
    public <T> void setJoinMessages(TaskId currentTaskId, JoinTaskMessage<T> joinTaskMessage) throws IOException {
        UUID taskToJoinId = currentTaskId.getId();
        UUID joinTaskId = joinTaskMessage.getTaskId().getId();

        Optional<TaskMessageEntity> joinTaskMessageOpt = taskMessageRepository.findByTaskToJoinIdAndJoinTaskId(
            taskToJoinId,
            joinTaskId
        );
        TaskMessageEntity taskMessageEntity;
        if (joinTaskMessageOpt.isPresent()) {
            taskMessageEntity = joinTaskMessageOpt.get();
        } else {
            boolean hasJoinTask = taskLinkRepository.checkBranchHasJoinTask(taskToJoinId, joinTaskId);
            if (!hasJoinTask) {
                throw new IllegalArgumentException("Task by=[%s] don't have a path to task=[%s]".formatted(
                    taskToJoinId, joinTaskId
                ));
            }
            taskMessageEntity = TaskMessageEntity.builder()
                .joinTaskId(joinTaskId)
                .taskToJoinId(taskToJoinId)
                .build();
        }

        byte[] bytes = taskSerializer.writeValue(joinTaskMessage.getMessage());
        taskMessageEntity = taskMessageEntity.toBuilder()
            .message(bytes)
            .build();
        taskMessageRepository.save(taskMessageEntity);
    }

    private <T> Collection<JoinTaskMessage<T>> readMessage(List<TaskMessageEntity> joinMessagesForBranch,
                                                           Map<UUID, TaskIdEntity> joinTaskIdToTaskIdEntities,
                                                           JavaType inputMessageType) throws IOException {
        List<JoinTaskMessage<T>> result = Lists.newArrayList();
        for (TaskMessageEntity taskMessageEntity : joinMessagesForBranch) {
            var joinTaskIdEntity = joinTaskIdToTaskIdEntities.get(taskMessageEntity.getJoinTaskId());
            result.add(readMessage(taskMessageEntity, joinTaskIdEntity, inputMessageType));
        }
        return result;
    }

    private <T> JoinTaskMessage<T> readMessage(TaskMessageEntity taskMessageEntity,
                                               TaskIdEntity joinTaskIdEntity,
                                               JavaType inputMessageType) throws IOException {
        T message = taskSerializer.readValue(taskMessageEntity.getMessage(), inputMessageType);
        return JoinTaskMessage.<T>builder()
            .message(message)
            .taskId(taskMapper.map(joinTaskIdEntity, commonSettings.getAppName()))
            .build();
    }

    private byte[] writeMessage(List<byte[]> rawJoinedMessages) throws IOException {
        return taskSerializer.writeValue(JoinTaskMessageContainer.builder()
            .rawMessages(rawJoinedMessages)
            .build()
        );
    }

    @Override
    public boolean hasLinks(UUID currentTaskId) {
        return taskLinkRepository.existsByTaskToJoinId(currentTaskId);
    }

    @Override
    public void markLinksAsCompleted(UUID currentTaskId) {
        taskLinkRepository.markLinksAsCompleted(currentTaskId);
    }

    @Override
    public List<UUID> getReadyToPlanJoinTasks(int batch) {
        return taskLinkRepository.getReadyToPlanJoinTasks(batch).stream()
            .map(UUIDEntity::getUuid)
            .toList();
    }

    @Override
    public List<JoinTaskExecution> prepareJoinTaskToPlan(List<UUID> joinTaskExecutionIds) {
        List<TaskMessageEntity> allMessages = taskMessageRepository.findAllByJoinTaskIdIn(joinTaskExecutionIds);
        taskMessageRepository.deleteAllByJoinTaskIdIn(JdbcTools.UUIDsToStringArray(joinTaskExecutionIds));
        taskLinkRepository.deleteAllByJoinTaskIdIn(JdbcTools.UUIDsToStringArray(joinTaskExecutionIds));

        Map<UUID, List<TaskMessageEntity>> messagesByJoinTaskId = allMessages.stream()
            .collect(Collectors.groupingBy(
                TaskMessageEntity::getJoinTaskId,
                Collectors.mapping(
                    Function.identity(),
                    Collectors.toList()
                )
            ));
        return joinTaskExecutionIds.stream()
            .map(joinTaskId -> joinMessages(
                joinTaskId,
                messagesByJoinTaskId.getOrDefault(joinTaskId, List.of())
            ))
            .toList();
    }

    //don't deserialize messages in order to join them, because joinPlanner
    //can have not class of message locally
    private JoinTaskExecution joinMessages(UUID joinTaskId, List<TaskMessageEntity> joinTaskLinkEntities) {
        List<byte[]> rawJoinedMessages = joinTaskLinkEntities.stream()
            .map(TaskMessageEntity::getMessage)
            .collect(Collectors.toList());
        byte[] joinedMessagesByte = null;
        try {
            joinedMessagesByte = writeMessage(rawJoinedMessages);
        } catch (IOException e) {
            log.error("joinMessages(): can't serialize message for joinTaskId=[{}], set empty", joinTaskId);
        }

        return JoinTaskExecution.builder()
            .taskId(joinTaskId)
            .joinedMessage(joinedMessagesByte)
            .build();
    }
}
