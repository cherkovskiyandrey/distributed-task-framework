package com.distributed_task_framework.test_service.tasks.mapreduce;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.tasks.BaseTask;
import com.distributed_task_framework.test_service.tasks.mapreduce.dto.MapDto;
import com.distributed_task_framework.test_service.tasks.mapreduce.dto.ReduceDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions.MAP_TASK;
import static com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions.REDUCE_TASK;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/*
Part of map/reduce flow: counts words in parallel.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class MapTask extends BaseTask<MapDto> {
    DistributedTaskService distributedTaskService;

    @Override
    public TaskDef<MapDto> getDef() {
        return MAP_TASK;
    }

    @Override
    public void execute(ExecutionContext<MapDto> executionContext) throws Exception {
        final Optional<MapDto> mapDtoOpt = executionContext.getInputMessageOpt();
        if (mapDtoOpt.isPresent()) {
            final MapDto mapDto = mapDtoOpt.get();
            final Map<String, Integer> wordsFreq = countWords(mapDto.words());

            final JoinTaskMessage<ReduceDto> msg = distributedTaskService.getJoinMessagesFromBranch(REDUCE_TASK).get(0);

            distributedTaskService.setJoinMessageToBranch(msg.toBuilder()
                .message(new ReduceDto(wordsFreq))
                .build());
        }
    }

    private Map<String, Integer> countWords(List<String> words) {
        return words.stream()
            .collect(toMap(identity(), w -> 1, Integer::sum));
    }
}
