package com.distributed_task_framework.test_service.tasks.mapreduce;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.test_service.tasks.BaseTask;
import com.distributed_task_framework.test_service.tasks.mapreduce.dto.ReduceDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions.REDUCE_TASK;
import static java.util.Comparator.reverseOrder;
import static java.util.Map.Entry.comparingByValue;

/*
Part of map/reduce flow: merges count results from map tasks and prints to output.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ReduceTask extends BaseTask<ReduceDto> {

    @Override
    public TaskDef<ReduceDto> getDef() {
        return REDUCE_TASK;
    }

    @Override
    public void execute(ExecutionContext<ReduceDto> executionContext) {
        final Map<String, Integer> wordTop = getWordsTop(
                // get all map task results
                mergeWordsFreq(executionContext.getInputJoinTaskMessages())
        );

        printResult(wordTop);
    }

    private void printResult(Map<String, Integer> wordTop) {
        System.out.println("Word top: " + wordTop);
    }

    private Map<String, Integer> getWordsTop(Map<String, Integer> wordFreq) {
        return wordFreq.entrySet()
                .stream()
                .sorted(comparingByValue(reverseOrder()))
                .limit(10)
                .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), LinkedHashMap::putAll);
    }

    private Map<String, Integer> mergeWordsFreq(List<ReduceDto> reduceDtos) {
        return reduceDtos.stream()
                .map(ReduceDto::wordFreq)
                .reduce(new HashMap<>(), (m1, m2) -> {
                    m2.forEach((k, v) -> m1.merge(k, v, Integer::sum));
                    return m1;
                });
    }
}
