package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaMethodDuplicateException;
import com.distributed_task_framework.saga.exceptions.SagaMethodNotFoundException;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.generator.TestSagaResolvingModelSpec;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.utils.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        //covariant example
        @Override
        public InheritanceReturnType parentInterfaceMethod(int i) {
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

    @Nested
    public class ResolveAsOperandTest {

        @Test
        void shouldHandleInheritanceOnPrevLevelWhenResolveAsOperandViaInterface() {
            //when
            var model = testSagaResolvingGenerator.<ParentInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .methodName("parentInterfaceMethod")
                .build()
            );

            //do
            var actualFirstSagaOperand = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) model.getTargetObject()::parentInterfaceMethod
            );

            //verify
            assertThat(actualFirstSagaOperand).isEqualTo(model.getSagaOperand());
        }

        @Test
        void shouldHandleInheritanceOnLastLevelWhenResolveAsOperandViaPrevLevel() {
            //when
            var model = testSagaResolvingGenerator.<Parent>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new SecondChild())
                .methodName("parentMethodToOverride")
                .build()
            );

            //do
            var actualSecondSagaOperand = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, Integer>) model.getTargetObject()::parentMethodToOverride
            );

            //verify
            assertThat(actualSecondSagaOperand).isEqualTo(model.getSagaOperand());
        }

        @Test
        void shouldHandleInheritanceOnLastLevelWithCovariantReturnTypeWhenResolveAsOperandViaInterface() {
            //when
            var model = testSagaResolvingGenerator.<ParentInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new SecondChild())
                .methodName("parentInterfaceMethod")
                .parameters(List.of(int.class))
                .returnType(InheritanceReturnType.class)
                .build()
            );

            //do
            var actualSecondSagaOperand = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) model.getTargetObject()::parentInterfaceMethod
            );

            //verify
            assertThat(actualSecondSagaOperand).isEqualTo(model.getSagaOperand());
        }

        @Test
        void shouldHandleInheritanceOnLastLevelWith2ExtendedClassesWhenResolveAsOperandViaPrevLevel() {
            //when
            var firstModel = testSagaResolvingGenerator.<Parent>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .methodName("parentInterfaceMethod")
                .build()
            );
            testSagaResolvingGenerator.<Parent>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new SecondChild())
                .methodName("parentInterfaceMethod")
                .build()
            );

            //do
            var actualSecondSagaOperand = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) firstModel.getTargetObject()::parentInterfaceMethod
            );

            //verify
            assertThat(actualSecondSagaOperand).isEqualTo(firstModel.getSagaOperand());
        }

        @Test
        void shouldHandleInheritanceOnLastLevelWith2BeansWhenResolveAsOperandViaInterface() {
            //when
            var firstModel = testSagaResolvingGenerator.<ParentInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .methodName("parentInterfaceMethod")
                .build()
            );
            var secondModel = testSagaResolvingGenerator.<ParentInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .methodName("parentInterfaceMethod")
                .build()
            );

            //do
            var actualFirstOperand = resolveAsOperandHelper(firstModel.getTargetObject());
            var actualSecondOperand = resolveAsOperandHelper(secondModel.getTargetObject());

            //verify
            assertThat(actualFirstOperand).isEqualTo(firstModel.getSagaOperand());
            assertThat(actualSecondOperand).isEqualTo(secondModel.getSagaOperand());
        }

        private SagaOperand resolveAsOperandHelper(ParentInterface targetObject) {
            return sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) targetObject::parentInterfaceMethod
            );
        }

        @Test
        void shouldHandleInheritanceOnLastLevelWhenResolveAsOperandViaBranchedInterface() {
            //when
            var model = testSagaResolvingGenerator.<ChildInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .methodName("childInterfaceMethod")
                .build()
            );

            //do
            var actualFirstSagaOperand = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, Integer>) model.getTargetObject()::childInterfaceMethod
            );

            //verify
            assertThat(actualFirstSagaOperand).isEqualTo(model.getSagaOperand());
        }

        @Test
        void shouldHandleInheritanceOnPrevLevelWhenResolveAsOperandViaInterfaceAndProxyObjects() {
            //when
            var model = testSagaResolvingGenerator.<ParentInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .withProxy(true)
                .methodName("parentInterfaceMethod")
                .build()
            );

            //do
            var actualSagaOperandSpyOne = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) model.getProxy()::parentInterfaceMethod
            );

            //verify
            assertThat(actualSagaOperandSpyOne).isEqualTo(model.getSagaOperand());
        }
    }

    @Nested
    public class FindMethodInObjectTest {

        @Test
        void shouldFindMethodInObjectTest() {
            //when
            var model = testSagaResolvingGenerator.<Parent>generate(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .methodName("parentInterfaceMethod")
                .build()
            );

            //do
            var actualMethod = sagaResolver.findMethodInObject(
                (SagaFunction<Integer, ReturnType>) model.getTargetObject()::parentInterfaceMethod,
                model.getTargetObject()
            );

            //verify
            assertThat(actualMethod).isEqualTo(model.getMethod());
        }
    }

    @Nested
    public class RegisterUnregisterOperandTest {

        @Test
        void shouldThrowExceptionWhenDuplicateName() {
            //when
            var model = testSagaResolvingGenerator.<ParentInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .methodName("parentInterfaceMethod")
                .build()
            );

            //do & verify
            assertThatThrownBy(() ->
                sagaResolver.registerOperand(model.getOperandName(), model.getSagaOperand().toBuilder().build())
            ).isInstanceOf(SagaMethodDuplicateException.class);
        }

        @Test
        void shouldThrowExceptionWhenAlreadyRegisteredAndProxy() {
            //when
            var model = testSagaResolvingGenerator.<ParentInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .withProxy(true)
                .methodName("parentInterfaceMethod")
                .build()
            );

            //do & verify
            assertThatThrownBy(() ->
                sagaResolver.registerOperand(model.getOperandName(), model.getSagaOperand().toBuilder()
                    .targetObject(model.getProxy())
                    .proxyWrappers(List.of())
                    .build()
                )
            ).isInstanceOf(SagaMethodDuplicateException.class);
        }

        @Test
        void shouldThrowExceptionWhenAlreadyRegisteredAsOverrideMethod() {
            //when
            var methodName = "parentInterfaceMethod";
            var model = testSagaResolvingGenerator.<ParentInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .lookupInClass(ParentInterface.class)
                .methodName(methodName)
                .build()
            );
            var overrideMethod = TestUtils.findFirstMethod(Parent.class, methodName);

            //do & verify
            assertThatThrownBy(() ->
                sagaResolver.registerOperand(model.getOperandName(), model.getSagaOperand().toBuilder()
                    .method(overrideMethod)
                    .build()
                )
            ).isInstanceOf(SagaMethodDuplicateException.class);
        }

        @Test
        void shouldThrowExceptionWhenAlreadyRegisteredAsOverrideCovariantMethod() {
            //when
            var methodName = "parentInterfaceMethod";
            var model = testSagaResolvingGenerator.<ParentInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new SecondChild())
                .lookupInClass(ParentInterface.class)
                .methodName(methodName)
                .build()
            );
            var overrideMethod = TestUtils.findMethod(
                SecondChild.class,
                methodName,
                InheritanceReturnType.class,
                List.of(int.class)
            );

            //do & verify
            assertThatThrownBy(() ->
                sagaResolver.registerOperand(model.getOperandName(), model.getSagaOperand().toBuilder()
                    .method(overrideMethod)
                    .build()
                )
            ).isInstanceOf(SagaMethodDuplicateException.class);
        }

        @Test
        void shouldRegisterOperand() {
            //when
            var model = testSagaResolvingGenerator.<ParentInterface>generate(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .lookupInClass(ParentInterface.class)
                .methodName("parentInterfaceMethod")
                .build()
            );
            Parent firstChildAsParent = (Parent) model.getTargetObject();
            FirstChild firstChild = (FirstChild) model.getTargetObject();

            //do
            sagaResolver.registerOperand("test", model.getSagaOperand());

            //verify
            var actualSagaOperandAsParentInterface = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) model.getTargetObject()::parentInterfaceMethod
            );
            assertThat(actualSagaOperandAsParentInterface).isEqualTo(model.getSagaOperand());

            var actualSagaOperandAsParent = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) firstChildAsParent::parentInterfaceMethod
            );
            assertThat(actualSagaOperandAsParent).isEqualTo(model.getSagaOperand());

            var actualSagaOperand = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) firstChild::parentInterfaceMethod
            );
            assertThat(actualSagaOperand).isEqualTo(model.getSagaOperand());
        }

        @Test
        void shouldRegisterOperandWithSeveralMethods() {
            //when
            FirstChild firstChild = new FirstChild();
            var firstModel = testSagaResolvingGenerator.<FirstChild>generate(TestSagaResolvingModelSpec.builder()
                .object(firstChild)
                .methodName("parentInterfaceMethod")
                .build()
            );
            var secondModel = testSagaResolvingGenerator.<FirstChild>generate(TestSagaResolvingModelSpec.builder()
                .object(firstChild)
                .methodName("parentMethodToOverride")
                .build()
            );

            //do
            sagaResolver.registerOperand("firstOperand", firstModel.getSagaOperand());
            sagaResolver.registerOperand("secondOperand", secondModel.getSagaOperand());

            //verify
            var actualFirstSagaOperand = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) firstChild::parentInterfaceMethod
            );
            assertThat(actualFirstSagaOperand).isEqualTo(firstModel.getSagaOperand());

            var actualSecondSagaOperand = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, Integer>) firstChild::parentMethodToOverride
            );
            assertThat(actualSecondSagaOperand).isEqualTo(secondModel.getSagaOperand());
        }

        @Test
        void shouldUnregisterOperand() {
            //when
            var model = testSagaResolvingGenerator.<ParentInterface>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(new FirstChild())
                .methodName("parentInterfaceMethod")
                .build()
            );
            Parent firstChildAsParent = (Parent) model.getTargetObject();
            FirstChild firstChild = (FirstChild) model.getTargetObject();

            //do
            sagaResolver.unregisterOperand(model.getOperandName());

            //verify
            assertThatThrownBy(() -> sagaResolver.resolveAsOperand(
                    (SagaFunction<Integer, ReturnType>) model.getTargetObject()::parentInterfaceMethod
                )
            ).isInstanceOf(SagaMethodNotFoundException.class);

            assertThatThrownBy(() -> sagaResolver.resolveAsOperand(
                    (SagaFunction<Integer, ReturnType>) firstChildAsParent::parentInterfaceMethod
                )
            ).isInstanceOf(SagaMethodNotFoundException.class);

            assertThatThrownBy(() -> sagaResolver.resolveAsOperand(
                    (SagaFunction<Integer, ReturnType>) firstChild::parentInterfaceMethod
                )
            ).isInstanceOf(SagaMethodNotFoundException.class);
        }

        @Test
        void shouldUnregisterOperandWithSeveralMethods() {
            //when
            FirstChild firstChild = new FirstChild();
            var firstModel = testSagaResolvingGenerator.<FirstChild>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(firstChild)
                .methodName("parentInterfaceMethod")
                .build()
            );
            ParentInterface firstChildAsParentInterface = firstModel.getTargetObject();
            Parent firstChildAsParent = firstModel.getTargetObject();

            var secondModel = testSagaResolvingGenerator.<FirstChild>generateAndReg(TestSagaResolvingModelSpec.builder()
                .object(firstChild)
                .methodName("parentMethodToOverride")
                .build()
            );

            //do
            sagaResolver.unregisterOperand(secondModel.getOperandName());

            //verify
            var actualSagaOperandAsParentInterface = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) firstChildAsParentInterface::parentInterfaceMethod
            );
            assertThat(actualSagaOperandAsParentInterface).isEqualTo(firstModel.getSagaOperand());

            var actualSagaOperandAsParent = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) firstChildAsParent::parentInterfaceMethod
            );
            assertThat(actualSagaOperandAsParent).isEqualTo(firstModel.getSagaOperand());

            var actualSagaOperand = sagaResolver.resolveAsOperand(
                (SagaFunction<Integer, ReturnType>) firstChild::parentInterfaceMethod
            );
            assertThat(actualSagaOperand).isEqualTo(firstModel.getSagaOperand());

            assertThatThrownBy(() -> sagaResolver.resolveAsOperand(
                    (SagaFunction<Integer, Integer>) firstChildAsParent::parentMethodToOverride
                )
            ).isInstanceOf(SagaMethodNotFoundException.class);

            assertThatThrownBy(() -> sagaResolver.resolveAsOperand(
                    (SagaFunction<Integer, Integer>) firstChild::parentMethodToOverride
                )
            ).isInstanceOf(SagaMethodNotFoundException.class);
        }
    }
}