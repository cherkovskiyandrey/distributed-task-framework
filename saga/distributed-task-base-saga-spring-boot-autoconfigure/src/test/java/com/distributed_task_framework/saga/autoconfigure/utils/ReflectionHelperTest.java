package com.distributed_task_framework.saga.autoconfigure.utils;

import com.distributed_task_framework.saga.autoconfigure.BaseSpringIntegrationTest;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.aopalliance.intercept.Joinpoint;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.aopalliance.intercept.MethodInterceptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


@FieldDefaults(level = AccessLevel.PRIVATE)
class ReflectionHelperTest extends BaseSpringIntegrationTest {

    public interface TestInterface {
        void foo();
    }

    public static class TestBean implements TestInterface{
        @Override
        public void foo() {
        }
    }


    @Test
    void shouldReturnOriginalObject() {
        //when
        var testBean = new TestBean();

        //do
        var realProxyObject = ReflectionHelper.unwrapSpringBean(testBean);

        //verify
        assertThat(realProxyObject).isEqualTo(new ReflectionHelper.ProxyObject(testBean, List.of()));
    }

    @Test
    void shouldReturnOriginalObjectWhenCglib() {
        //when
        var testBean = new TestBean();
        var proxyBean = simpleCglibProxyFor(testBean);

        //do
        var realProxyObject = ReflectionHelper.unwrapSpringBean(proxyBean);

        //verify
        assertThat(realProxyObject).isEqualTo(new ReflectionHelper.ProxyObject(testBean, List.of(proxyBean)));
    }

    @Test
    void shouldReturnOriginalObjectWhenCglibAndTelescopic() {
        //when
        var testBean = new TestBean();
        var proxyBean1 = simpleCglibProxyFor(testBean);
        var proxyBean2 = simpleCglibProxyFor(proxyBean1);
        var proxyBean3 = simpleCglibProxyFor(proxyBean2);

        //do
        var realProxyObject = ReflectionHelper.unwrapSpringBean(proxyBean3);

        //verify
        assertThat(realProxyObject).isEqualTo(new ReflectionHelper.ProxyObject(
                testBean,
                List.of(proxyBean3, proxyBean2, proxyBean1)
            )
        );
    }

    @Test
    void shouldReturnOriginalObjectWhenJdkProxy() {
        //when
        var testBean = new TestBean();
        var proxyBean = simpleJdkProxyFor(testBean, TestInterface.class);

        //do
        var realProxyObject = ReflectionHelper.unwrapSpringBean(proxyBean);

        //verify
        assertThat(realProxyObject).isEqualTo(new ReflectionHelper.ProxyObject(testBean, List.of(proxyBean)));
    }

    @Test
    void shouldReturnOriginalObjectWhenJdkProxyAndTelescopic() {
        //when
        var testBean = new TestBean();
        var proxyBean1 = simpleJdkProxyFor(testBean, TestInterface.class);
        var proxyBean2 = simpleJdkProxyFor(proxyBean1, TestInterface.class);
        var proxyBean3 = simpleJdkProxyFor(proxyBean2, TestInterface.class);

        //do
        var realProxyObject = ReflectionHelper.unwrapSpringBean(proxyBean3);

        //verify
        assertThat(realProxyObject).isEqualTo(new ReflectionHelper.ProxyObject(
                testBean,
                List.of(proxyBean3, proxyBean2, proxyBean1)
            )
        );
    }

    @SuppressWarnings("unchecked")
    private <T, U extends T> T simpleJdkProxyFor(U bean, Class<T> interfaceClass) {
        var proxyFactory = new ProxyFactory();
        proxyFactory.setTarget(bean);
        proxyFactory.setInterfaces(interfaceClass);
        proxyFactory.setProxyTargetClass(false);
        proxyFactory.addAdvice((MethodInterceptor) Joinpoint::proceed);
        T proxy = (T) proxyFactory.getProxy();
        assertThat(proxy).isNotEqualTo(bean);

        return proxy;
    }

    @SuppressWarnings("unchecked")
    private <T> T simpleCglibProxyFor(T bean) {
        var proxyFactory = new ProxyFactory();
        proxyFactory.setTarget(bean);
        proxyFactory.setProxyTargetClass(true);
        proxyFactory.addAdvice((MethodInterceptor) Joinpoint::proceed);
        T proxy = (T) proxyFactory.getProxy();
        assertThat(proxy).isNotEqualTo(bean);

        return proxy;
    }
}