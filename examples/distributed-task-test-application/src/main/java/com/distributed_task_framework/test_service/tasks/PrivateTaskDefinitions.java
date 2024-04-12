package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.test_service.tasks.dto.ComplexMessageDto;
import com.distributed_task_framework.test_service.tasks.dto.JoinTaskLevelOne;
import com.distributed_task_framework.test_service.tasks.dto.JoinTaskLevelTwoDto;
import com.distributed_task_framework.test_service.tasks.mapreduce.dto.MapReduceDto;
import com.distributed_task_framework.test_service.tasks.dto.SimpleMessageDto;
import com.distributed_task_framework.test_service.tasks.mapreduce.dto.MapDto;
import com.distributed_task_framework.test_service.tasks.mapreduce.dto.ReduceDto;

//common shared definition of tasks related to one workflow
public interface PrivateTaskDefinitions {
    // Map/Reduce job example
    String MAP_REDUCE_AFFINITY_GROUP = "MAP_REDUCE_AFFINITY_GROUP";

    TaskDef<MapReduceDto> MAP_REDUCE_PARENT_TASK = TaskDef.privateTaskDef("MAP_REDUCE_PARENT_TASK", MapReduceDto.class);
    TaskDef<MapDto> MAP_TASK = TaskDef.privateTaskDef("MAP_TASK", MapDto.class);
    TaskDef<ReduceDto> REDUCE_TASK = TaskDef.privateTaskDef("REDUCE_TASK", ReduceDto.class);

    //---- Example of simple task flow ------
    TaskDef<SimpleMessageDto> SIMPLE_CONSOLE_OUTPUT_TASK_DEF = TaskDef.privateTaskDef("SIMPLE_CONSOLE_OUTPUT", SimpleMessageDto.class);

    TaskDef<ComplexMessageDto> SIMPLE_CONSOLE_OUTPUT_2_TASK_DEF = TaskDef.privateTaskDef("SIMPLE_CONSOLE_OUTPUT_2", ComplexMessageDto.class);
    //----------------------------------------

    //---- Example of cron task flow ------
    TaskDef<Void> CRON_TEST_TASK_DEF = TaskDef.privateTaskDef("CRON_TEST_TASK_DEF", Void.class);
    //----------------------------------------


    //---- Example of DAG with map-reduce ------
    //                                                               PARENT_TASK
    //                                                        /                      \
    //                                   /                                                                 \
    //               CHILD_TASK_LEVEL_ONE                                                                   CHILD_TASK_LEVEL_ONE
    //          /                       \                                                                    /                  \
    //CHILD_TASK_LEVEL_TWO      CHILD_TASK_LEVEL_TWO                                        CHILD_TASK_LEVEL_TWO             CHILD_TASK_LEVEL_TWO
    //              \                   /   \                                                               \                     /           \
    //               JOIN_TASK_LEVEL_TWO    CHILD_TASK_LEVEL_THREE (INDEPENDENT BRANCH)                        JOIN_TASK_LEVEL_TWO             CHILD_TASK_LEVEL_THREE (INDEPENDENT BRANCH)
    //                      |                                                                                           |
    //                 CHILD_FROM_JOIN_TASK                                                                     CHILD_FROM_JOIN_TASK
    //                                     \                                                                     /
    //                                                        \                                       /
    //                                                                  JOIN_TASK_LEVEL_ONE
    TaskDef<SimpleMessageDto> PARENT_TASK = TaskDef.privateTaskDef("PARENT_TASK", SimpleMessageDto.class);

    TaskDef<SimpleMessageDto> CHILD_TASK_LEVEL_ONE = TaskDef.privateTaskDef("CHILD_TASK_LEVEL_ONE", SimpleMessageDto.class);

    TaskDef<SimpleMessageDto> CHILD_TASK_LEVEL_TWO = TaskDef.privateTaskDef("CHILD_TASK_LEVEL_TWO", SimpleMessageDto.class);

    TaskDef<SimpleMessageDto> CHILD_TASK_LEVEL_THREE_INDEPENDENT = TaskDef.privateTaskDef("CHILD_TASK_LEVEL_THREE_INDEPENDENT", SimpleMessageDto.class);

    TaskDef<JoinTaskLevelTwoDto> JOIN_TASK_LEVEL_TWO = TaskDef.privateTaskDef("JOIN_TASK_LEVEL_TWO", JoinTaskLevelTwoDto.class);

    TaskDef<SimpleMessageDto> CHILD_FROM_JOIN_TASK = TaskDef.privateTaskDef("CHILD_FROM_JOIN_TASK", SimpleMessageDto.class);

    TaskDef<JoinTaskLevelOne> JOIN_TASK_LEVEL_ONE = TaskDef.privateTaskDef("JOIN_TASK_LEVEL_ONE", JoinTaskLevelOne.class);
    //----------------------------


    TaskDef<Void> TEST_OOM_TASK = TaskDef.privateTaskDef("TEST_OOM_TASK", Void.class);

    TaskDef<Void> CHECK_TIMEOUT_TASK = TaskDef.privateTaskDef("CHECK_TIMEOUT_TASK", Void.class);
}
