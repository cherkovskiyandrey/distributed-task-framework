package com.distributed_task_framework.test_service.tasks;

import com.distributed_task_framework.test_service.tasks.dto.ComplexMessageDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.task.Task;

import java.util.UUID;

@Slf4j
//@TaskSchedule(cron = "*/5 * * * * *")
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
@Component
public class SimpleCronTask implements Task<Void> {
    DistributedTaskService distributedTaskService;

    @Override
    public TaskDef<Void> getDef() {
        return PrivateTaskDefinitions.CRON_TEST_TASK_DEF;
    }

    @Override
    public void execute(ExecutionContext<Void> executionContext) throws Exception {
        log.info("===>>>>>> {}", executionContext.getCurrentTaskId());
        distributedTaskService.schedule(
                PrivateTaskDefinitions.SIMPLE_CONSOLE_OUTPUT_2_TASK_DEF,
                ExecutionContext.simple(ComplexMessageDto.builder()
                        .addBalance(125)
                        .userId(UUID.randomUUID())
                        .build()
                )
        );
    }

    @Override
    public void onFailure(FailedExecutionContext<Void> failedExecutionContext) {
        log.error("ERROR: {}", failedExecutionContext.toString());
    }
}
