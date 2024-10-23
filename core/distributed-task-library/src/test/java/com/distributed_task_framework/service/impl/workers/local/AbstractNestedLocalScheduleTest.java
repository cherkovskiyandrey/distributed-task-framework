package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskGenerator;
import com.distributed_task_framework.task.TestTaskModelSpec;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractNestedLocalScheduleTest extends BaseLocalWorkerIntegrationTest {

    @ParameterizedTest
    @EnumSource(ActionMode.class)
    void shouldScheduleNewTask(ActionMode actionMode) {
        //when
        setFixedTime();
        var childTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var parentTestTaskModel = buildActionAndGenerateTask(
            ctx -> {
                var innerTask = distributedTaskService.schedule(childTestTaskModel.getTaskDef(), ExecutionContext.empty());
                assertThat(taskRepository.find(innerTask.getId())).isEmpty();
            },
            String.class,
            actionMode
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        verifyLocalTaskIsFinished(parentTestTaskModel.getTaskEntity());
        verifyOnlyOneTask(childTestTaskModel.getTaskDef());
    }

    @Test
    void shouldWinOnFailureWhenScheduleNewTask() {
        //when
        setFixedTime();
        var childFromActionTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var childFromFailureTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                var innerTask = distributedTaskService.schedule(childFromActionTestTaskModel.getTaskDef(), ExecutionContext.empty());
                assertThat(taskRepository.find(innerTask.getId())).isEmpty();
                throw new RuntimeException();
            })
            .failureAction(ctx -> {
                var innerTask = distributedTaskService.schedule(childFromFailureTestTaskModel.getTaskDef(), ExecutionContext.empty());
                assertThat(taskRepository.find(innerTask.getId())).isEmpty();
                return true;
            })
            .build()
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        verifyLocalTaskIsFinished(parentTestTaskModel.getTaskEntity());
        verifyIsEmptyByTaskDef(childFromActionTestTaskModel.getTaskDef());
        verifyOnlyOneTask(childFromFailureTestTaskModel.getTaskDef());
    }

    @Test
    void shouldNotScheduleNewTaskWhenNotLastOnFailure() {
        //when
        setFixedTime();
        var childFromActionTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var childFromFailureTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                var innerTask = distributedTaskService.schedule(childFromActionTestTaskModel.getTaskDef(), ExecutionContext.empty());
                assertThat(taskRepository.find(innerTask.getId())).isEmpty();
                throw new RuntimeException();
            })
            .failureAction(ctx -> {
                var innerTask = distributedTaskService.schedule(childFromFailureTestTaskModel.getTaskDef(), ExecutionContext.empty());
                assertThat(taskRepository.find(innerTask.getId())).isEmpty();
                return false;
            })
            .build()
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        verifyTaskInNextAttempt(parentTestTaskModel.getTaskId(), parentTestTaskModel.getTaskSettings());
        verifyIsEmptyByTaskDef(childFromActionTestTaskModel.getTaskDef());
        verifyIsEmptyByTaskDef(childFromFailureTestTaskModel.getTaskDef());
    }

    @Test
    void shouldNotScheduleNewTaskWhenLastOnFailureAndException() {
        //when
        setFixedTime();
        var childFromFailureTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .taskEntityCustomizer(extendedTaskGenerator.withLastAttempt())
            .action(TestTaskModelSpec.throwException())
            .failureAction(ctx -> {
                distributedTaskService.schedule(childFromFailureTestTaskModel.getTaskDef(), ExecutionContext.empty());
                throw new RuntimeException();
            })
            .build()
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        verifyLocalTaskIsFinished(parentTestTaskModel.getTaskEntity());
        verifyIsEmptyByTaskDef(childFromFailureTestTaskModel.getTaskDef());
    }

    //todo: all test below should be rewritten like for both ActionMode

    @Test
    void shouldScheduleImmediatelyNewTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            TaskId innerTask = distributedTaskService.scheduleImmediately(taskDef, ExecutionContext.simple("childTaskOne"));
            assertThat(taskRepository.find(innerTask.getId())).isPresent();
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        assertThat(taskRepository.findByName("test", 2)).singleElement()
            .matches(te -> te.getVersion() == 1, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "schedule time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker");
    }
}
