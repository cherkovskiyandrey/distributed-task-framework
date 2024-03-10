package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.test_service.tasks.dto.ComplexMessageDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.test_service.tasks.dto.SimpleMessageDto;


@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
@Component
public class SimpleConsoleOutputTask implements Task<SimpleMessageDto> {
    DistributedTaskService distributedTaskService;

    @Override
    public TaskDef<SimpleMessageDto> getDef() {
        return PrivateTaskDefinitions.SIMPLE_CONSOLE_OUTPUT_TASK_DEF;
    }

    @Override
    public void execute(ExecutionContext<SimpleMessageDto> executionContext) throws Exception {
        log.info("context: {}", executionContext);
        distributedTaskService.schedule(
                PrivateTaskDefinitions.SIMPLE_CONSOLE_OUTPUT_2_TASK_DEF,
                executionContext.withNewMessage(ComplexMessageDto.builder().build())
        );
    }

    @Override
    public void onFailure(FailedExecutionContext<SimpleMessageDto> failedExecutionContext) {
        System.out.println("It's not a problem :)");
    }
}
