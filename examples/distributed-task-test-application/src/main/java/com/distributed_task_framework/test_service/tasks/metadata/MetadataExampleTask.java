package com.distributed_task_framework.test_service.tasks.metadata;

import com.distributed_task_framework.autoconfigure.annotation.TaskCreationInterceptors;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionInterceptors;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.Metadata;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.test_service.tasks.PrivateTaskDefinitions;
import com.distributed_task_framework.test_service.tasks.dto.ComplexMessageDto;
import com.distributed_task_framework.test_service.tasks.dto.SimpleMessageDto;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
@Component
@TaskCreationInterceptors(TraceIdCreationInterceptor.class)
@TaskExecutionInterceptors(LoggingExecutionInterceptor.class)
public class MetadataExampleTask implements Task<SimpleMessageDto> {
    DistributedTaskService distributedTaskService;

    @Override
    public TaskDef<SimpleMessageDto> getDef() {
        return PrivateTaskDefinitions.METADATA_EXAMPLE_TASK_DEF;
    }

    @Override
    public void execute(ExecutionContext<SimpleMessageDto> executionContext) throws Exception {
        // metadata is set via API and by TraceIdCreationInterceptor
        Metadata metadata = executionContext.getMetadata();
        log.info("metadata example: traceId=[{}], tenant=[{}], tags=[{}]",
            metadata.getSingle("traceId").orElse("unknown"),
            metadata.getSingle("tenant").orElse("unknown"),
            metadata.get("tags")
        );

        // metadata is inherited by child tasks scheduled with withXXX methods
        distributedTaskService.schedule(
            PrivateTaskDefinitions.SIMPLE_CONSOLE_OUTPUT_2_TASK_DEF,
            executionContext.withNewMessage(ComplexMessageDto.builder().build())
        );
    }

    @Override
    public void onFailure(FailedExecutionContext<SimpleMessageDto> failedExecutionContext) {
        log.error("metadata example failed: {}", failedExecutionContext.toString());
    }
}
