package com.distributed_task_framework.test.autoconfigure.tasks;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.test.autoconfigure.Signaller;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;

@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class CpuIntensiveExampleTask implements Task<Void> {
    private static final int SIZE = 1 * 1024 * 1024;
    private static final Duration TIME_TO_EXECUTE = Duration.ofSeconds(3);
    public static final TaskDef<Void> TASK_DEF = TaskDef.privateTaskDef("cpu-intensive");

    Signaller signaller;

    @Override
    public TaskDef<Void> getDef() {
        return TASK_DEF;
    }

    @Override
    public void execute(ExecutionContext<Void> executionContext) throws Exception {
        signaller.getCyclicBarrierRef().get().await();
        long currentTime = System.currentTimeMillis();
        do {
            int[] array = new int[SIZE]; // 4 Mb
            for (int i = 0; i < SIZE; ++i) {
                array[i] = SIZE - i;
            }
            Arrays.sort(array);
            log.info("array.length = {}, firstElement={}", array.length, array[0]);
        } while (Duration.ofMillis(System.currentTimeMillis() - currentTime).compareTo(TIME_TO_EXECUTE) < 0);
    }
}
