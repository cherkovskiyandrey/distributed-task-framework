package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.common.RemoteStubTask;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.TaskSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import com.distributed_task_framework.task.TaskGenerator;

import static org.assertj.core.api.Assertions.assertThat;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class DistributedTaskServiceIntegrationTest extends BaseSpringIntegrationTest {
    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    DistributedTaskService distributedTaskService;
    @Autowired
    TaskRepository taskRepository;
    @Autowired
    ObjectMapper objectMapper;

    @Test
    void contextShouldBeLoad() {
    }

    @Test
    void shouldRegisterLocalTask() {
        //when
        Task<String> task = TaskGenerator.defineTask(
                TaskDef.privateTaskDef("task", String.class),
                m -> System.out.println("hello world!")
        );

        //do
        distributedTaskService.registerTask(task);

        //verify
        Assertions.assertThat(distributedTaskService.getRegisteredTask(task.getDef()))
                .isPresent()
                .get()
                .matches(regTask -> TaskSettings.DEFAULT.equals(regTask.getTaskSettings()), "taskParameters")
                .matches(regTask -> task.equals(regTask.getTask()), "task");
    }

    @Test
    void shouldRegisterTask() {
        //when
        Task<String> task = TaskGenerator.defineTask(
                TaskDef.publicTaskDef("test-app", "task", String.class),
                m -> System.out.println("hello world!")
        );

        //do
        distributedTaskService.registerTask(task);

        //verify
        Assertions.assertThat(distributedTaskService.getRegisteredTask(task.getDef()))
                .isPresent()
                .get()
                .matches(regTask -> TaskSettings.DEFAULT.equals(regTask.getTaskSettings()), "taskParameters")
                .matches(regTask -> task.equals(regTask.getTask()), "task");
    }

    @Test
    void shouldRegisterTaskWithCustomParameters() {
        //when
        Task<String> task = TaskGenerator.defineTask(
                TaskDef.privateTaskDef("task", String.class),
                m -> System.out.println("hello world!")
        );
        TaskSettings taskSettings = defaultTaskSettings.toBuilder()
                .maxParallelInCluster(555)
                .build();

        //do
        distributedTaskService.registerTask(task, taskSettings);

        //verify
        Assertions.assertThat(distributedTaskService.getRegisteredTask(task.getDef()))
                .isPresent()
                .get()
                .matches(regTask -> taskSettings.equals(regTask.getTaskSettings()), "taskParameters")
                .matches(regTask -> task.equals(regTask.getTask()), "task");
    }

    @Test
    void shouldRegisterRemoteTask() {
        //when
        TaskDef<String> taskDef = TaskDef.publicTaskDef("foreign-app", "test", String.class);
        TaskSettings taskSettings = TaskSettings.DEFAULT.toBuilder().build();

        //do
        distributedTaskService.registerRemoteTask(taskDef);

        //verify
        Assertions.assertThat(distributedTaskService.getRegisteredTask(taskDef))
                .isPresent()
                .get()
                .matches(regTask -> taskSettings.equals(regTask.getTaskSettings()), "taskParameters")
                .matches(regTask -> RemoteStubTask.class.equals(regTask.getTask().getClass()), "task");
    }

    @Test
    void shouldRegisterRemoteTaskWithParameters() {
        //when
        TaskDef<String> taskDef = TaskDef.publicTaskDef("foreign-app", "test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder()
                .maxParallelInCluster(555)
                .build();

        //do
        distributedTaskService.registerRemoteTask(taskDef, taskSettings);

        //verify
        Assertions.assertThat(distributedTaskService.getRegisteredTask(taskDef))
                .isPresent()
                .get()
                .matches(regTask -> taskSettings.equals(regTask.getTaskSettings()), "taskParameters")
                .matches(regTask -> RemoteStubTask.class.equals(regTask.getTask().getClass()), "task");
    }

    //todo: TaskCommandService api to cover for local tasks and for remote tasks
    //todo: validation tests

//
//    @SneakyThrows
//    @Test
//    void shouldScheduleImmediately() {
//        //do
//        TaskId taskId = distributedTaskService.schedule(simpleTask.getDef(), ExecutionContext.simple("hello world!"));
//
//        //verify
//        assertThat(taskRepository.find(taskId))
//                .isPresent()
//                .get()
//                .matches(taskEntity -> LocalDateTime.now(ZoneOffset.UTC).isEqual(taskEntity.getExecutionDateUtc()) ||
//                        LocalDateTime.now(ZoneOffset.UTC).isAfter(taskEntity.getExecutionDateUtc()), "ExecutionDate")
//                .matches(taskEntity -> {
//                    try {
//                        return "hello world!".equals(objectMapper.readValue(taskEntity.getMessageBytes(), String.class));
//                    } catch (IOException e) {
//                        throw new RuntimeException();
//                    }
//                }, "MessageBytes")
//                .matches(taskEntity -> taskEntity.getFailures() == 0, "failures")
//        ;
//    }
//
//    @SneakyThrows
//    @Test
//    void shouldScheduleWithDelay() {
//        //do
//        TaskId taskId = distributedTaskService.schedule(simpleTask.getDef(), ExecutionContext.simple("hello world!"), Duration.ofHours(1));
//
//        //verify
//        assertThat(taskRepository.find(taskId))
//                .isPresent()
//                .get()
//                .matches(taskEntity -> LocalDateTime.now(ZoneOffset.UTC).isBefore(taskEntity.getExecutionDateUtc().minusMinutes(10)),
//                        "ExecutionDate")
//        ;
//    }

}
