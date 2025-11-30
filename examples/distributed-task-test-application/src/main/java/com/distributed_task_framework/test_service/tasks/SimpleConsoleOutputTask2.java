package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.test_service.tasks.dto.ComplexMessageDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
@Component
public class SimpleConsoleOutputTask2 implements Task<ComplexMessageDto> {

    @Override
    public TaskDef<ComplexMessageDto> getDef() {
        return PrivateTaskDefinitions.SIMPLE_CONSOLE_OUTPUT_2_TASK_DEF;
    }

    @Override
    public void execute(ExecutionContext<ComplexMessageDto> executionContext) {
        log.info("message: {}", executionContext.getInputMessageOrThrow());
    }

    @Override
    public void onFailure(FailedExecutionContext<ComplexMessageDto> failedExecutionContext) {
        System.out.println("It's not a problem :)");
    }
}
