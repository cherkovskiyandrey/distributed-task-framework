package com.distributed_task_framework.saga.autoconfigure;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.util.ReflectionUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 *
 * Pure class without any dependencies, especially MeterRegistry in order to prevent
 * early creating o these beans. For example for MeterRegistry,
 * early creating leads to a major of metrics will not be created.
 * </p>
 * Only not lazy singletons are taken into account here. Laziness is detected by bean definition
 * metadata, so no bean is created as a side effect of the discovery itself.
 * <p>
 * Saga methods of lazy beans are silently not registered: it isn't possible to detect them reliably without
 * creating beans, because a bean definition doesn't always know the real class behind it (a {@code @Bean} method
 * declaring a supertype, a {@link org.springframework.beans.factory.FactoryBean} product, and so on).
 * Hence saga beans have to be not lazy singletons, otherwise saga work isn't guaranteed. Pay attention that
 * {@code spring.main.lazy-initialization=true} makes every bean definition lazy and as a result disables
 * saga registration completely.
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaBeanCollector implements BeanPostProcessor, ApplicationContextAware, EnvironmentAware {
    private static final Set<String> IGNORE_METHOD_NAMES = Set.of(
        "equals",
        "hashCode",
        "toString",
        "wait",
        "notify",
        "getClass",
        "notifyAll",
        "finalize",
        "clone"
    );

    @NonFinal
    ConfigurableListableBeanFactory configurableListableBeanFactory;
    @Getter
    Set<String> sagaBeanNames = ConcurrentHashMap.newKeySet();

    @Override
    public void setEnvironment(Environment environment) {
        tryToCheckGlobalLaziness(environment);
    }

    private void tryToCheckGlobalLaziness(Environment environment) {
        var isGlobalLazyInit = Binder.get(environment)
            .bind("spring.main.lazy-initialization", Boolean.class)
            .orElse(false);
        if (isGlobalLazyInit) {
            log.warn("""
!!!!!!!!!!!!!!!!!!!!!!!! spring.main.lazy-initialization=true !!!!!!!!!!!!!!!!!!!!!!!!!!
!!              !If you have saga beans, they will not be registered!                 !!
!!         !Saga beans are registered only for eagerly created singletons beans!      !!
!!           !Turn off lazy-initialization for correct working of application!        !!
!!                      !If you don't have saga beans at all,                         !!
!!         consider to exclude 'distributed-task-base-saga-spring-boot-autoconfigure' !!
!!                             dependency from you project!                           !!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                """);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        configurableListableBeanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (isAcceptableBean(bean, beanName)) {
            sagaBeanNames.add(beanName);
        }
        return bean;
    }

    private boolean isAcceptableBean(Object bean, String beanName) {
        if (!configurableListableBeanFactory.containsBeanDefinition(beanName)
            || bean instanceof FactoryBean<?>
            || !configurableListableBeanFactory.isSingleton(beanName)) {
            return false;
        }

        BeanDefinition beanDefinition = configurableListableBeanFactory.getMergedBeanDefinition(beanName);
        //bean definition is not factoryBean, neither a lazy singleton, neither abstract,
        //hence it has already been created and initialised
        //during context refresh: getBean() below is only a singleton cache lookup and never triggers initialisation
        return beanDefinition.isSingleton()
            && !beanDefinition.isAbstract()
            && !beanDefinition.isLazyInit();
    }

    public static Stream<Method> findAnnotatedMethods(Class<?> beanType, Class<? extends Annotation> annotationCls) {
        return Arrays.stream(ReflectionUtils.getUniqueDeclaredMethods(beanType))
            .filter(SagaBeanCollector::isNotIgnoredMethod)
            .filter(method -> com.distributed_task_framework.autoconfigure.utils.ReflectionHelper.findAnnotation(method, annotationCls).isPresent());
    }

    private static boolean isNotIgnoredMethod(Method method) {
        return IGNORE_METHOD_NAMES.stream()
            .noneMatch(methodName -> method.getName().equals(methodName));
    }
}
