package com.distributed_task_framework.utils;

import com.google.common.collect.Lists;
import lombok.experimental.UtilityClass;
import org.springframework.util.CollectionUtils;
import com.distributed_task_framework.service.internal.BatcheableLocalCommand;
import com.distributed_task_framework.service.internal.LocalCommand;

import java.util.List;
import java.util.stream.Collectors;

@UtilityClass
public class CommandHelper {

    public List<LocalCommand> collapseToBatchedCommands(List<LocalCommand> localCommands) {
        var result = Lists.<LocalCommand>newArrayList();

        var batchedPartitionedCommands = localCommands.stream()
                .collect(Collectors.partitioningBy(localCommand -> localCommand instanceof BatcheableLocalCommand));
        result.addAll(batchedPartitionedCommands.get(false));

        @SuppressWarnings("unchecked")
        var groupedByBatchedClass = batchedPartitionedCommands.get(true).stream()
                .map(localCommand -> (BatcheableLocalCommand<LocalCommand>) localCommand)
                .collect(Collectors.groupingBy(BatcheableLocalCommand::batchClass));

        List<LocalCommand> batchedLocalCommands = groupedByBatchedClass.keySet().stream()
                .map(groupedByBatchedClass::get)
                .filter(cmds -> !CollectionUtils.isEmpty(cmds))
                .map(CommandHelper::makeBatchForOneGroup)
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
