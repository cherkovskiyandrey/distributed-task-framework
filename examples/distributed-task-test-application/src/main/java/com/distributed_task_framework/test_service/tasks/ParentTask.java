package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.tasks.dto.JoinTaskLevelOne;
import com.distributed_task_framework.test_service.tasks.dto.SimpleMessageDto;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.settings.TaskSettings;

import java.util.List;

@Slf4j
@Component
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class ParentTask extends BaseTask<SimpleMessageDto> {
    DistributedTaskService distributedTaskService;

    @Override
    public TaskDef<SimpleMessageDto> getDef() {
        return PrivateTaskDefinitions.PARENT_TASK;
    }

    @Override
    public void execute(ExecutionContext<SimpleMessageDto> executionContext) throws Exception {
        log.info("execute(): start task");
        List<TaskId> joinList = Lists.newArrayList();
        for (int i = 0; i < 10; ++i) {
            TaskId taskId = distributedTaskService.schedule(
                    PrivateTaskDefinitions.CHILD_TASK_LEVEL_ONE,
                    ExecutionContext.simple(SimpleMessageDto.builder()
                            .message("" + i)
                            .build()
                    )
            );
            joinList.add(taskId);
        }

        //schedule join
        distributedTaskService.scheduleJoin(
                PrivateTaskDefinitions.JOIN_TASK_LEVEL_ONE,
                executionContext.withNewMessage(JoinTaskLevelOne.builder().build()),
                joinList
        );
        log.info("execute(): stop task");
    }
}
