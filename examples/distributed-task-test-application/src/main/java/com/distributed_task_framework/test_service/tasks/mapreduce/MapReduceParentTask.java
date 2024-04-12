package com.distributed_task_framework.test_service.tasks.mapreduce;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.tasks.BaseTask;
import com.distributed_task_framework.test_service.tasks.mapreduce.dto.MapDto;
import com.distributed_task_framework.test_service.tasks.mapreduce.dto.MapReduceDto;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions.MAP_REDUCE_PARENT_TASK;
import static com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions.MAP_TASK;
import static com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions.REDUCE_TASK;

/*
Computes top 10 most frequent words using map/reduce approach.
It has few steps:
1. Prepare. Build word collections for map tasks (lowercase, remove signs), schedule map tasks and join task,
    which would be a reduce task.
2. Map tasks do a work counting job.
3. Reduce task merges results from map tasks and prints to output.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class MapReduceParentTask extends BaseTask<MapReduceDto> {
    private static final int DEFAULT_MAP_TASKS = 5;

    DistributedTaskService distributedTaskService;

    @Override
    public TaskDef<MapReduceDto> getDef() {
        return MAP_REDUCE_PARENT_TASK;
    }

    @Override
    public void execute(ExecutionContext<MapReduceDto> executionContext) throws Exception {
        final MapReduceDto mrDto = executionContext.getInputMessageOpt()
                .orElseGet(() -> new MapReduceDto("default", DEFAULT_MAP_TASKS));

        // Split text onto partitions depending on desired number of tasks.
        final List<String> words = toWords(mrDto.text());
        final List<List<String>> partitions = Lists.partition(words, mrDto.tasks()); // guava library
        final List<TaskId> tasksToJoin = new ArrayList<>(partitions.size());
        for (List<String> partition : partitions) {
            // Schedule map tasks and keep taskIds to use for join task.
            final TaskId taskId = distributedTaskService.schedule(
                    MAP_TASK,
                    executionContext.withNewMessage(new MapDto(partition)));

            tasksToJoin.add(taskId);
        }

        // Wait for all map tasks to be completed.
        distributedTaskService.scheduleJoin(
                REDUCE_TASK,
                executionContext.withEmptyMessage(),
                tasksToJoin);
    }

    private List<String> toWords(String text) {
        return Arrays.stream(text.split("\n"))
                .map(line -> line.toLowerCase().replaceAll("\\p{Punct}", " "))
                .flatMap(line -> Arrays.stream(line.split(" ")))
                .map(String::strip)
                .filter(w -> !w.isBlank())
                .toList();
    }
}
