package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.utils.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.ReflectionUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

//todo:
class SagaResolverImplTest extends BaseSpringIntegrationTest {

    static class ReturnType {
    }

    static class InheritanceReturnType extends ReturnType {
    }

    interface ParentInterface {

        ReturnType parentInterfaceMethod(int i);

        default int defaultParentSample(int i) {
            return i;
        }
    }

    static class Parent implements ParentInterface {

        @Override
        public ReturnType parentInterfaceMethod(int i) {
            //common logic is here
            return null;
        }

        public int parentMethod(int i) {
            return i;
        }

        public int parentMethodToOverride(int i) {
            return i;
        }
    }

    interface ChildInterface {

        int childInterfaceMethod(int i);

        default int defaultChildSample(int i) {
            return i;
        }
    }

    static class FirstChild extends Parent implements ChildInterface {

        @Override
        public int childInterfaceMethod(int i) {
            return i;
        }

        @Override
        public int parentMethodToOverride(int i) {
            return i * 2;
        }
    }

    static class SecondChild extends Parent implements ChildInterface {

        //must be declared also here in order to eliminate uncertainly
        //between FirstChild and SecondChild, because saga task is linked to method and certain object
        @Override
        public InheritanceReturnType parentInterfaceMethod(int i) {
            //just to invoke logic
            return null;
        }

        @Override
        public int childInterfaceMethod(int i) {
            return i;
        }

        @Override
        public int parentMethodToOverride(int i) {
            return i * 3;
        }
    }

    @BeforeEach
    @AfterEach
    public void init() {
        super.init();
        sagaResolver.getAllRegisteredOperandNames().forEach(sagaResolver::unregisterOperand);
    }

    @Test
    void shouldHandleInheritanceOnPrevLevelWhenResolveAsOperandViaInterface() {
        //when
        ParentInterface firstChild = new FirstChild();

        var methodName = "parentInterfaceMethod";
        var firstChildMethod = TestUtils.findMethod(
            SecondChild.class,
            methodName,
            ReturnType.class,
            List.of(int.class)
        );
        var firstChildTask = TaskDef.privateTaskDef("FirstChild#" + methodName + "#task", SagaPipeline.class);
        var firstChildSagaOperand = SagaOperand.builder()
            .taskDef(firstChildTask)
            .targetObject(firstChild)
            .method(firstChildMethod)
            .build();
        sagaResolver.registerOperand("FirstChild#" + methodName, firstChildSagaOperand);

        //do
        var actualFirstSagaOperand = sagaResolver.resolveAsOperand(
            (SagaFunction<Integer, ReturnType>) firstChild::parentInterfaceMethod
        );

        //verify
        assertThat(actualFirstSagaOperand).isEqualTo(firstChildSagaOperand);
    }

    @Test
    void shouldHandleInheritanceOnLastLevelWhenResolveAsOperandViaPrevLevel() {
        //when
        Parent secondChild = new SecondChild();

        var methodName = "parentMethodToOverride";
        var secondChildMethod = ReflectionUtils.findMethod(SecondChild.class, methodName, int.class);
        var secondChildTask = TaskDef.privateTaskDef("SecondChild#" + methodName + "#task", SagaPipeline.class);
        var secondChildSagaOperand = SagaOperand.builder()
            .taskDef(secondChildTask)
            .targetObject(secondChild)
            .method(secondChildMethod)
            .build();
        sagaResolver.registerOperand("SecondChild#" + methodName, secondChildSagaOperand);

        //do
        var actualSecondSagaOperand = sagaResolver.resolveAsOperand(
            (SagaFunction<Integer, Integer>) secondChild::parentMethodToOverride
        );

        //verify
        assertThat(actualSecondSagaOperand).isEqualTo(secondChildSagaOperand);
    }

    @Test
    void shouldHandleInheritanceOnLastLevelWithCovariantReturnTypeWhenResolveAsOperandViaInterface() {
        //when
        ParentInterface secondChild = new SecondChild();

        var methodName = "parentInterfaceMethod";
        var secondChildMethod = TestUtils.findMethod(
            SecondChild.class,
            methodName,
            InheritanceReturnType.class,
            List.of(int.class)
        );
        var secondChildTask = TaskDef.privateTaskDef("SecondChild#" + methodName + "#task", SagaPipeline.class);
        var secondChildSagaOperand = SagaOperand.builder()
            .taskDef(secondChildTask)
            .targetObject(secondChild)
            .method(secondChildMethod)
            .build();
        sagaResolver.registerOperand("SecondChild#" + methodName, secondChildSagaOperand);

        //do
        var actualSecondSagaOperand = sagaResolver.resolveAsOperand(
            (SagaFunction<Integer, ReturnType>) secondChild::parentInterfaceMethod
        );

        //verify
        assertThat(actualSecondSagaOperand).isEqualTo(secondChildSagaOperand);
    }

    @Test
    void shouldHandleInheritanceOnLastLevelWith2ExtendedClassesWhenResolveAsOperandViaPrevLevel() {
        //when
        Parent firstChild = new FirstChild();
        Parent secondChild = new SecondChild();

        var methodName = "parentInterfaceMethod";
        var parentMethod = TestUtils.findMethod(
            Parent.class,
            methodName,
            ReturnType.class,
            List.of(int.class)
        );

        var firstChildTask = TaskDef.privateTaskDef("FirstChild#" + methodName + "#task", SagaPipeline.class);
        var firstChildSagaOperand = SagaOperand.builder()
            .taskDef(firstChildTask)
            .targetObject(firstChild)
            .method(parentMethod)
            .build();
        sagaResolver.registerOperand("FirstChild#" + methodName, firstChildSagaOperand);

        var secondChildTask = TaskDef.privateTaskDef("SecondChild#" + methodName + "#task", SagaPipeline.class);
        var secondChildSagaOperand = SagaOperand.builder()
            .taskDef(secondChildTask)
            .targetObject(secondChild)
            .method(parentMethod)
            .build();
        sagaResolver.registerOperand("SecondChild#" + methodName, secondChildSagaOperand);

        //do
        var actualSecondSagaOperand = sagaResolver.resolveAsOperand(
            (SagaFunction<Integer, ReturnType>) secondChild::parentInterfaceMethod
        );

        //verify
        assertThat(actualSecondSagaOperand).isEqualTo(secondChildSagaOperand);
    }

    @Test
    void shouldHandleInheritanceOnLastLevelWith2BeansWhenResolveAsOperandViaInterface() {
        //when
        ParentInterface firstChild1 = new FirstChild();
        ParentInterface firstChild2 = new FirstChild();

        var methodName = "parentInterfaceMethod";
        var firstChildMethod = TestUtils.findMethod(
            FirstChild.class,
            methodName,
            ReturnType.class,
            List.of(int.class)
        );

        var firstChildTask1 = TaskDef.privateTaskDef("FirstChild1#" + methodName + "#task", SagaPipeline.class);
        var firstChildSagaOperand1 = SagaOperand.builder()
            .taskDef(firstChildTask1)
            .targetObject(firstChild1)
            .method(firstChildMethod)
            .build();
        sagaResolver.registerOperand("FirstChild1#" + methodName, firstChildSagaOperand1);

        var firstChildTask2 = TaskDef.privateTaskDef("FirstChild2#" + methodName + "#task", SagaPipeline.class);
        var firstChildSagaOperand2 = SagaOperand.builder()
            .taskDef(firstChildTask2)
            .targetObject(firstChild2)
            .method(firstChildMethod)
            .build();
        sagaResolver.registerOperand("FirstChild2#" + methodName, firstChildSagaOperand2);

        //do
        var actualFirstSagaOperand2 = sagaResolver.resolveAsOperand(
            (SagaFunction<Integer, ReturnType>) firstChild2::parentInterfaceMethod
        );

        //verify
        assertThat(actualFirstSagaOperand2).isEqualTo(firstChildSagaOperand2);
    }


    //todo: default methods in interfaces

    //todo: findMethodInObject

    //--------------------------------------------------------

    //todo: with several methods in one bean - reg/unreg, call resolve
    @Test
    void registerOperandIntegrationTest() {
    }

    @Test
    void resolveByTaskName() {
    }
}