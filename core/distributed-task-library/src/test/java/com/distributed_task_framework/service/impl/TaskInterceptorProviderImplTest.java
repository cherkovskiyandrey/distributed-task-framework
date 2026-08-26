package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.interceptor.CommonTaskCreationInterceptor;
import com.distributed_task_framework.interceptor.CommonTaskExecutionInterceptor;
import com.distributed_task_framework.interceptor.TaskCreationInterceptor;
import com.distributed_task_framework.interceptor.TaskExecutionChain;
import com.distributed_task_framework.interceptor.TaskExecutionInterceptor;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.settings.TaskSettings;
import org.junit.jupiter.api.Test;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskInterceptorProviderImplTest {

    interface UnknownCreationInterceptor extends TaskCreationInterceptor {
    }

    static class CreationFirst implements TaskCreationInterceptor {
        @Override
        public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
            return executionContext;
        }
    }

    static class CommonCreationFirst implements CommonTaskCreationInterceptor {
        @Override
        public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
            return executionContext;
        }
    }

    static class CommonCreationSecond implements CommonTaskCreationInterceptor {
        @Override
        public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
            return executionContext;
        }
    }

    static class CommonExecutionFirst implements CommonTaskExecutionInterceptor {
        @Override
        public <U> void execute(ExecutionContext<U> executionContext, TaskDef<U> taskDef, TaskExecutionChain chain) {
        }
    }

    static class CreationSecond implements TaskCreationInterceptor {
        @Override
        public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
            return executionContext;
        }
    }

    @Order(1)
    static class CreationOrdered implements TaskCreationInterceptor {
        @Override
        public <U> ExecutionContext<U> onTaskCreated(ExecutionContext<U> executionContext, TaskDef<U> taskDef) {
            return executionContext;
        }
    }

    static class ExecutionFirst implements TaskExecutionInterceptor {
        @Override
        public <U> void execute(ExecutionContext<U> executionContext, TaskDef<U> taskDef, TaskExecutionChain chain) {
        }
    }

    @Order(Ordered.HIGHEST_PRECEDENCE)
    static class ExecutionOrdered implements TaskExecutionInterceptor {
        @Override
        public <U> void execute(ExecutionContext<U> executionContext, TaskDef<U> taskDef, TaskExecutionChain chain) {
        }
    }

    @Test
    void shouldResolveInterceptorsByClass() {
        //when
        TaskInterceptorProviderImpl provider = new TaskInterceptorProviderImpl(
            List.of(new CreationFirst(), new CreationSecond()),
            List.of(new ExecutionFirst())
        );

        //do
        List<TaskCreationInterceptor> creation = provider.getCreationInterceptors(TaskSettings.builder()
            .creationInterceptors(List.of(CreationSecond.class))
            .build()
        );
        List<TaskExecutionInterceptor> execution = provider.getExecutionInterceptors(TaskSettings.builder()
            .executionInterceptors(List.of(ExecutionFirst.class))
            .build()
        );

        //verify
        assertThat(creation).hasSize(1).first().isInstanceOf(CreationSecond.class);
        assertThat(execution).hasSize(1).first().isInstanceOf(ExecutionFirst.class);
    }

    @Test
    void shouldResolveInterceptorsByBaseClass() {
        //when
        TaskInterceptorProviderImpl provider = new TaskInterceptorProviderImpl(
            List.of(new CreationFirst()),
            List.of(new ExecutionFirst())
        );

        //do
        List<TaskCreationInterceptor> creation = provider.getCreationInterceptors(TaskSettings.builder()
            .creationInterceptors(List.of(TaskCreationInterceptor.class))
            .build()
        );

        //verify
        assertThat(creation).hasSize(1).first().isInstanceOf(CreationFirst.class);
    }

    @Test
    void shouldFailFastWhenConfiguredInterceptorIsNotRegistered() {
        //when
        TaskInterceptorProviderImpl provider = new TaskInterceptorProviderImpl(
            List.of(new CreationFirst()),
            List.of()
        );

        //do/verify
        assertThatThrownBy(() -> provider.getCreationInterceptors(TaskSettings.builder()
            .creationInterceptors(List.of(UnknownCreationInterceptor.class))
            .build()
        ))
            .isInstanceOf(TaskConfigurationException.class)
            .hasMessageContaining(UnknownCreationInterceptor.class.getName())
            .hasMessageContaining(CreationFirst.class.getName());
    }

    @Test
    void shouldSortInterceptorsByOrder() {
        //when
        TaskInterceptorProviderImpl provider = new TaskInterceptorProviderImpl(
            List.of(new CreationSecond(), new CreationFirst(), new CreationOrdered()),
            List.of()
        );

        //do
        List<TaskCreationInterceptor> creation = provider.getCreationInterceptors(TaskSettings.builder()
            .creationInterceptors(List.of(
                CreationFirst.class,
                CreationOrdered.class,
                CreationSecond.class
            ))
            .build()
        );

        //verify
        assertThat(creation)
            .extracting(Object::getClass)
            .containsExactly(
                CreationOrdered.class,
                CreationFirst.class,
                CreationSecond.class
            );
    }

    @Test
    void shouldReturnEmptyListForEmptyConfig() {
        //when
        TaskInterceptorProviderImpl provider = new TaskInterceptorProviderImpl(List.of(), List.of());

        //do
        List<TaskCreationInterceptor> creation = provider.getCreationInterceptors(TaskSettings.DEFAULT);
        List<TaskExecutionInterceptor> execution = provider.getExecutionInterceptors(TaskSettings.DEFAULT);

        //verify
        assertThat(creation).isEmpty();
        assertThat(execution).isEmpty();
    }

    @Test
    void shouldApplyCommonInterceptorsWithoutConfig() {
        //when
        TaskInterceptorProviderImpl provider = new TaskInterceptorProviderImpl(
            List.of(new CommonCreationFirst(), new CreationFirst()),
            List.of(new CommonExecutionFirst())
        );

        //do
        List<TaskCreationInterceptor> creation = provider.getCreationInterceptors(TaskSettings.DEFAULT);
        List<TaskExecutionInterceptor> execution = provider.getExecutionInterceptors(TaskSettings.DEFAULT);

        //verify
        assertThat(creation)
            .extracting(Object::getClass)
            .containsExactly(CommonCreationFirst.class);
        assertThat(execution)
            .extracting(Object::getClass)
            .containsExactly(CommonExecutionFirst.class);
    }

    @Test
    void shouldExcludeCommonInterceptorsPerTask() {
        //when
        TaskInterceptorProviderImpl provider = new TaskInterceptorProviderImpl(
            List.of(new CommonCreationFirst(), new CommonCreationSecond(), new CreationFirst()),
            List.of()
        );

        //do
        List<TaskCreationInterceptor> creation = provider.getCreationInterceptors(TaskSettings.builder()
            .excludedCommonCreationInterceptors(List.of(CommonCreationFirst.class))
            .build()
        );

        //verify
        assertThat(creation)
            .extracting(Object::getClass)
            .containsExactly(CommonCreationSecond.class);
    }

    @Test
    void shouldExcludeCommonInterceptorsByBaseClass() {
        //when
        TaskInterceptorProviderImpl provider = new TaskInterceptorProviderImpl(
            List.of(new CommonCreationFirst(), new CommonCreationSecond()),
            List.of()
        );

        //do
        List<TaskCreationInterceptor> creation = provider.getCreationInterceptors(TaskSettings.builder()
            .excludedCommonCreationInterceptors(List.of(CommonTaskCreationInterceptor.class))
            .build()
        );

        //verify
        assertThat(creation).isEmpty();
    }
}
