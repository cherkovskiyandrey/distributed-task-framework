package com.distributed_task_framework.utils;

import com.distributed_task_framework.local_commands.BatcheableLocalCommand;
import com.distributed_task_framework.local_commands.LocalCommand;
import com.distributed_task_framework.local_commands.TaskBasedLocalCommand;
import com.distributed_task_framework.local_commands.TaskDefBasedLocalCommand;
import com.distributed_task_framework.local_commands.WorkflowBasedLocalCommand;
import com.distributed_task_framework.local_commands.impl.AbstractTaskBasedContextAwareCommand;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import lombok.experimental.UtilityClass;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@UtilityClass
public class LocalCommandHelper {

    public List<LocalCommand> collapseToBatchedCommands(List<LocalCommand> localCommands) {
        var result = Lists.<LocalCommand>newArrayList();

        var taskIdToNumber = Maps.<UUID, Integer>newHashMap();
        var taskNames = Sets.<String>newHashSet();
        var workflowIds = Sets.newHashSet();

        for (var command : localCommands) {
            if (command instanceof TaskDefBasedLocalCommand cmd) {
                taskNames.add(cmd.taskDef().getTaskName());
            } else if (command instanceof WorkflowBasedLocalCommand cmd) {
                workflowIds.addAll(cmd.workflows());
            } else if (command instanceof TaskBasedLocalCommand cmd) {
                taskIdToNumber.compute(cmd.taskEntity().getId(), (key, value) -> value == null ? 1 : value + 1);
            }
        }

        var batcheableLocalCommand = Lists.newArrayList();
        var taskBasedContextAwareCommandsByTaskId = MultimapBuilder.hashKeys()
            .arrayListValues()
            .<UUID, AbstractTaskBasedContextAwareCommand>build();

        for (var command : localCommands) {
            // independent batcheable task based command
            if (command instanceof BatcheableLocalCommand<?> batchCmd
                && batchCmd instanceof TaskBasedLocalCommand taskBaseCmd
                && taskIdToNumber.getOrDefault(taskBaseCmd.taskEntity().getId(), 1) == 1
                && !taskNames.contains(taskBaseCmd.taskEntity().getTaskName())
                && !workflowIds.contains(taskBaseCmd.taskEntity().getWorkflowId())) {
                batcheableLocalCommand.add(batchCmd);

                // batcheable NOT task based command
            } else if (command instanceof BatcheableLocalCommand<?> batchCmd
                && !(batchCmd instanceof TaskBasedLocalCommand)) {
                batcheableLocalCommand.add(batchCmd);

                // task based context aware command
                //We don't check against taskNames and workflowIds because now commands
                //were we have to check opt-locking and have to has context are result current task.
                //Grouped commands like ...ByTaskDefCommand and ...ByWorkflowCommand split to
                // 2 groups: for current task and for others. For current task it is always
                //commands implements TaskBasedLocalCommand
            } else if (command instanceof AbstractTaskBasedContextAwareCommand taskBaseCmd
                && taskIdToNumber.getOrDefault(taskBaseCmd.taskEntity().getId(), 1) > 1) {
                taskBasedContextAwareCommandsByTaskId.put(taskBaseCmd.taskEntity().getId(), taskBaseCmd);

            } else {
                result.add(command);
            }
        }

        var taskBasedAwareCommands = taskBasedContextAwareCommandsByTaskId.asMap().values().stream()
            .filter(commands -> !commands.isEmpty())
            .map(commands -> commands.stream()
                .reduce(AbstractTaskBasedContextAwareCommand::doAfter)
                .orElseThrow()
            ).toList();
        result.addAll(taskBasedAwareCommands);

        @SuppressWarnings("unchecked")
        var groupedByBatchedClass = batcheableLocalCommand.stream()
            .map(localCommand -> (BatcheableLocalCommand<LocalCommand>) localCommand)
            .collect(Collectors.groupingBy(BatcheableLocalCommand::batchClass));

        List<LocalCommand> batchedLocalCommands = groupedByBatchedClass.keySet().stream()
            .map(groupedByBatchedClass::get)
            .filter(cmds -> !CollectionUtils.isEmpty(cmds))
            .map(LocalCommandHelper::makeBatchForOneGroup)
            .toList();
        result.addAll(batchedLocalCommands);

        return result;
    }

    private <T extends LocalCommand> T makeBatchForOneGroup(List<BatcheableLocalCommand<T>> batcheableLocalCommands) {
        T batch = null;
        for (var cmd : batcheableLocalCommands) {
            batch = cmd.addToBatch(batch);
        }
        return batch;
    }
}
