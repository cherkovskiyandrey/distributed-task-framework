package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.model.DltTask;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.task.Task;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.distributed_task_framework.autoconfigure.DisabledDistributedTaskAutoconfigure.DUMMY_TASK_ID;
import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBootTest(
        properties = {
                "distributed-task.enabled=false",
                "distributed-task.common.app-name=test"
        })
@EnableAutoConfiguration
@ContextConfiguration(classes = {
        DistributedTaskAutoconfigure.class
})
public class DisabledDtfTest {
    @Autowired
    DistributedTaskService distributedTaskService;

    @Test
    void shouldDoNothingOnRegisterTask() {
        // do
        distributedTaskService.registerTask(new Task<>() {
            @Override
            public TaskDef<Object> getDef() {
                return null;
            }

            @Override
            public void execute(ExecutionContext<Object> executionContext) {

            }
        });
    }

    @Test
    void shouldDoNothingOnGetRegisteredTask() {
        // do
        final Optional<RegisteredTask<Object>> empty = distributedTaskService.getRegisteredTask("task");

        // verify
        assertThat(empty).isEmpty();
    }

    @Test
    void shouldReturnFalseOnIsTaskRegistered() {
        // do
        final boolean registered = distributedTaskService.isTaskRegistered("task");

        // verify
        assertThat(registered).isFalse();
    }

    @Test
    void shouldReturnEmptyOnGetDltTaskDefs() {
        // do
        final Collection<TaskDef<?>> dltTaskDefs = distributedTaskService.getDltTaskDefs();

        // verify
        assertThat(dltTaskDefs).isEmpty();
    }

    @Test
    void shouldReturnEmptyPageOnGetDltTasksByDef() {
        // do
        final Page<DltTask<?>> res = distributedTaskService.getDltTasksByDef(null, null);

        // verify
        assertThat(res.isEmpty()).isTrue();
    }

    @Test
    void shouldReturnDummyOnSchedule() throws Exception {
        // do
        final TaskId scheduled = distributedTaskService.schedule(null, null);

        // verify
        assertThat(scheduled).isEqualTo(DUMMY_TASK_ID);
    }

    @Test
    void shouldReturnDummyOnScheduleImmediately() throws Exception {
        // do
        final TaskId scheduled = distributedTaskService.scheduleImmediately(null, null);

        // verify
        assertThat(scheduled).isEqualTo(DUMMY_TASK_ID);
    }

    @Test
    void shouldReturnEmptyListOnGetJoinMessagesFromBranch() throws Exception {
        // do
        final List<JoinTaskMessage<Object>> res = distributedTaskService.getJoinMessagesFromBranch(null);

        // verify
        assertThat(res).isEmpty();
    }
}
