package com.distributed_task_framework.perf_test.tasks;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestGeneratedSpecDto;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestIntermediateDto;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestIntermediateResultDto;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestResultDto;
import com.distributed_task_framework.task.Task;

public interface PerfTestTaskDefinitions<T> extends Task<T> {

    //                                                              GENERATED_TASK
    //                                                                    /
    //                                                               PARENT_TASK
    //                                                        /                      \
    //                                   /                                                                 \
    //               CHILD_TASK_LEVEL_ONE                                                                   CHILD_TASK_LEVEL_ONE
    //          /                       \                                                                    /                  \
    //CHILD_TASK_LEVEL_TWO      CHILD_TASK_LEVEL_TWO                                        CHILD_TASK_LEVEL_TWO             CHILD_TASK_LEVEL_TWO
    //              \                   /                                                                   \                     /
    //               JOIN_TASK_LEVEL_TWO                                                                       JOIN_TASK_LEVEL_TWO
    //                      |                                                                                           |
    //                 CHILD_FROM_JOIN_TASK                                                                     CHILD_FROM_JOIN_TASK
    //                                     \                                                                     /
    //                                                        \                                       /
    //                                                                  JOIN_TASK_LEVEL_ONE
    TaskDef<PerfTestGeneratedSpecDto> STRESS_TEST_GENERATED_TASK = TaskDef.privateTaskDef("STRESS_TEST_GENERATED_TASK", PerfTestGeneratedSpecDto.class);

    TaskDef<PerfTestIntermediateDto> STRESS_TEST_PARENT_TASK = TaskDef.privateTaskDef(
            "STRESS_TEST_PARENT_TASK",
            PerfTestIntermediateDto.class
    );

    TaskDef<PerfTestIntermediateDto> STRESS_TEST_CHILD_TASK_LEVEL_ONE = TaskDef.privateTaskDef(
            "STRESS_TEST_CHILD_TASK_LEVEL_ONE",
            PerfTestIntermediateDto.class
    );

    TaskDef<PerfTestIntermediateDto> STRESS_TEST_CHILD_TASK_LEVEL_TWO = TaskDef.privateTaskDef(
            "STRESS_TEST_CHILD_TASK_LEVEL_TWO",
            PerfTestIntermediateDto.class
    );

    TaskDef<PerfTestIntermediateResultDto> STRESS_TEST_JOIN_TASK_LEVEL_TWO = TaskDef.privateTaskDef(
            "STRESS_TEST_JOIN_TASK_LEVEL_TWO",
            PerfTestIntermediateResultDto.class
    );

    TaskDef<PerfTestResultDto> STRESS_TEST_JOIN_TASK_LEVEL_ONE = TaskDef.privateTaskDef(
            "STRESS_TEST_JOIN_TASK_LEVEL_ONE",
            PerfTestResultDto.class
    );
}
