package com.distributed_task_framework.utils;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.core.annotation.Order;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DistributedTaskLifecycleSpringConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(DistributedTaskLifecycleSpringConfiguration.class));

    @BeforeEach
    void setUp() {
        TestServiceTracker.reset();
    }

    @AfterEach
    void tearDown() {
        TestServiceTracker.reset();
    }

    @Test
    void shouldCreateAndDestroyBeansInCorrectOrder() {
        contextRunner
            .withUserConfiguration(TestServicesConfiguration.class)
            .run(context -> {
                    // Verify context started successfully
                    assertThat(context)
                        .hasNotFailed()
                        .hasBean("distributedTaskLifecycleSpringInitializer")
                        .hasBean("serviceA")
                        .hasBean("serviceB")
                        .hasBean("serviceC");

                    // Verify init order: A -> B -> C (bean creation order)
                    assertThat(TestServiceTracker.getInitOrder())
                        .containsExactly("serviceA", "serviceB", "serviceC");

                    // Verify start order: A -> B -> C (same as creation order)
                    assertThat(TestServiceTracker.getStartOrder())
                        .containsExactly("serviceA", "serviceB", "serviceC");
                }
            );

        // After context is closed, verify destroy order
        // stop order should be reverse: C -> B -> A
        assertThat(TestServiceTracker.getStopOrder())
            .containsExactly("serviceC", "serviceB", "serviceA");

        // cleanup order should be reverse: C -> B -> A
        assertThat(TestServiceTracker.getCleanupOrder())
            .containsExactly("serviceC", "serviceB", "serviceA");
    }

    @Test
    void shouldHaveInitializerBean() {
        contextRunner
            .run(context -> assertThat(context)
                .hasNotFailed()
                .hasBean("distributedTaskLifecycleSpringInitializer")
                .getBean("distributedTaskLifecycleSpringInitializer")
                .isInstanceOf(DistributedTaskLifecycleSpringInitializer.class)
            );
    }

    @Test
    void shouldIgnoreNonLifecycleBeans() {
        contextRunner
            .withUserConfiguration(MixedServicesConfiguration.class)
            .run(context -> {
                assertThat(context)
                    .hasNotFailed()
                    .hasBean("serviceA")
                    .hasBean("regularBean")
                    .hasBean("distributedTaskLifecycleSpringInitializer");

                // Only lifecycle beans should be tracked
                assertThat(TestServiceTracker.getInitOrder())
                    .containsExactly("serviceA")
                    .doesNotContain("regularBean");
            });
    }

    @Test
    void shouldHandleEmptyContext() {
        contextRunner
            .run(context -> {
                assertThat(context)
                    .hasNotFailed()
                    .hasBean("distributedTaskLifecycleSpringInitializer");

                assertThat(TestServiceTracker.getInitOrder()).isEmpty();
                assertThat(TestServiceTracker.getStartOrder()).isEmpty();
            });
    }

    @Nested
    class LazyDependencies {

        //todo
        @Test
        void shouldHandleIndependentLazyDependencyBeanInRuntime() {
            contextRunner
                .withUserConfiguration(LazyDependenciesInRuntime.class)
                .run(context -> {
                        // Verify context started successfully
                        assertThat(context)
                            .hasNotFailed()
                            .hasBean("firstService");

                        assertThat(TestServiceTracker.getPostConstructOrder()).containsExactly("firstService", "secondService");
                        assertThat(TestServiceTracker.getInitOrder()).containsExactlyElementsOf(TestServiceTracker.getPostConstructOrder());
                        assertThat(TestServiceTracker.getStartOrder()).containsExactlyElementsOf(TestServiceTracker.getPostConstructOrder());

                        var eagerService = context.getBean("firstService", TestService.class);
                        assertThat(eagerService).isNotNull();

                        assertThat(eagerService.getName()).isEqualTo("firstService");
                    }
                );

            assertThat(TestServiceTracker.getPreDestroyOrder()).containsExactly("firstService", "secondService");
            assertThat(TestServiceTracker.getStopOrder()).containsExactlyElementsOf(TestServiceTracker.getPreDestroyOrder());
            assertThat(TestServiceTracker.getCleanupOrder()).containsExactlyElementsOf(TestServiceTracker.getPreDestroyOrder());
        }

        @Test
        void shouldHandleDependentLazyServiceWhenContextIsStartingInInit() {
            contextRunner
                .withUserConfiguration(LazyDependentServiceInStartingContextInInitConfiguration.class)
                .run(context -> {
                        // Verify context started successfully
                        assertThat(context)
                            .hasNotFailed()
                            .hasBean("distributedTaskLifecycleSpringInitializer")
                            .hasBean("eagerService");

                        assertThat(TestServiceTracker.getInitOrder()).containsExactly("eagerService", "lazyService");
                        assertThat(TestServiceTracker.getStartOrder()).containsExactly("lazyService", "eagerService");
                    }
                );

            assertThat(TestServiceTracker.getStopOrder()).containsExactly("eagerService", "lazyService");
            assertThat(TestServiceTracker.getCleanupOrder()).containsExactly("eagerService", "lazyService");
        }

        //todo: to resolve problem on context destroying:
        // 1. 100% way: to delegate detection to spring framework:
        //  1.1 build graph on stop()
        //  1.2 detect and remove cycle dependencies on lazy injection
        // but!!! there is a problem: for circle dependencies spring doesn't make difference which dependency is first,
        // which a second! But we in dtf configuration does! And current detection DAG via postProcessAfterInitialization
        // can handle it for not Lazy beans with Lazy injection!
        // idea: detect and prohibit lazy beans. But: impossible, because we don't know type of bean if it hasn't been created yet!
        // idea: handle not lazy beans via postProcessAfterInitialization but not handle lazy beans, and
        //       make DAG on stop and merge with dependent list from postProcessAfterInitialization ???? - check it!
        @Test
        void shouldHandleLazyDependentServiceWhenContextIsStartingInStart() {
            contextRunner
                .withUserConfiguration(LazyDependentServiceInStartingContextInStartConfiguration.class)
                .run(context -> {
                        // Verify context started successfully
                        assertThat(context)
                            .hasNotFailed()
                            .hasBean("distributedTaskLifecycleSpringInitializer")
                            .hasBean("eagerService");

                        assertThat(TestServiceTracker.getPostConstructOrder()).containsExactly("eagerService", "lazyService");
                        assertThat(TestServiceTracker.getInitOrder()).containsExactly("eagerService", "lazyService");
                        assertThat(TestServiceTracker.getStartOrder()).containsExactly("eagerService", "lazyService");
                    }
                );

            assertThat(TestServiceTracker.getPreDestroyOrder()).containsExactly("eagerService", "lazyService");
            assertThat(TestServiceTracker.getStopOrder()).containsExactly("eagerService", "lazyService");
            assertThat(TestServiceTracker.getCleanupOrder()).containsExactly("eagerService", "lazyService");
        }

        //todo: to fix it see shouldHandleLazyInitBeanWhenContextIsStartingInStart
        @Test
        void shouldHandleLazyDependentServiceWhenContextIsStarted() {
            contextRunner
                .withUserConfiguration(LazyDependentServiceInRuntime.class)
                .run(context -> {
                        // Verify context started successfully
                        assertThat(context)
                            .hasNotFailed()
                            .hasBean("eagerService");

                        assertThat(TestServiceTracker.getPostConstructOrder()).containsExactly("eagerService");
                        assertThat(TestServiceTracker.getInitOrder()).containsExactly("eagerService");
                        assertThat(TestServiceTracker.getStartOrder()).containsExactly("eagerService");

                        var eagerService = context.getBean("eagerService", TestService.class);
                        assertThat(eagerService).isNotNull();

                        assertThat(eagerService.getName()).isEqualTo("eagerService");
                        assertThat(TestServiceTracker.getPostConstructOrder()).containsExactly("eagerService", "lazyService");
                        assertThat(TestServiceTracker.getInitOrder()).containsExactly("eagerService", "lazyService");
                        assertThat(TestServiceTracker.getStartOrder()).containsExactly("eagerService", "lazyService");
                    }
                );

            assertThat(TestServiceTracker.getPreDestroyOrder()).containsExactly("eagerService", "lazyService");
            assertThat(TestServiceTracker.getStopOrder()).containsExactly("eagerService", "lazyService");
            assertThat(TestServiceTracker.getCleanupOrder()).containsExactly("eagerService", "lazyService");
        }

        //todo
        @Test
        void shouldHandleLazyDependenciesWithCircleBeanWhenContextIsStarted() {
            contextRunner
                .withUserConfiguration(LazyDependenciesWithCircleServiceInRuntime.class)
                .run(context -> {
                        // Verify context started successfully
                        assertThat(context)
                            .hasNotFailed()
                            .hasBean("firstService")
                            .hasBean("secondService");

                        assertThat(TestServiceTracker.getPostConstructOrder()).containsExactly("firstService", "secondService");
                        assertThat(TestServiceTracker.getInitOrder()).containsExactly("firstService", "secondService");
                        assertThat(TestServiceTracker.getStartOrder()).containsExactly("firstService", "secondService");

                        var eagerService = context.getBean("firstService", TestService.class);
                        assertThat(eagerService).isNotNull();

                        assertThat(eagerService.getName()).isEqualTo("firstService");
                    }
                );

            assertThat(TestServiceTracker.getPreDestroyOrder()).containsExactly("secondService", "firstService");
            assertThat(TestServiceTracker.getStopOrder()).containsExactly("secondService", "firstService");
            assertThat(TestServiceTracker.getCleanupOrder()).containsExactly("secondService", "firstService");
        }

        //todo
        @Test
        void shouldHandleLazyDependenciesAndLazyBeanWithCircleBeanWhenContextIsStarted() {
            contextRunner
                .withUserConfiguration(LazyDependenciesAndLazyBeanWithCircleServiceInRuntime.class)
                .run(context -> {
                        // Verify context started successfully
                        assertThat(context)
                            .hasNotFailed()
                            .hasBean("firstService");

                        assertThat(TestServiceTracker.getInitOrder()).containsExactly("firstService");
                        assertThat(TestServiceTracker.getStartOrder()).containsExactly("firstService");

                        var eagerService = context.getBean("firstService", TestService.class);
                        assertThat(eagerService).isNotNull();

                        assertThat(eagerService.getName()).isEqualTo("firstService");
                    }
                );

            assertThat(TestServiceTracker.getStopOrder()).containsExactly("secondService", "firstService");
            assertThat(TestServiceTracker.getCleanupOrder()).containsExactly("secondService", "firstService");
        }

        @Test
        void shouldHandleLazyIndependentServiceWhenContextIsStarted() {
            contextRunner
                .withUserConfiguration(LazyServiceConfiguration.class)
                .run(context -> {
                        // Verify context started successfully
                        assertThat(context)
                            .hasNotFailed()
                            .hasBean("eagerService");

                        assertThat(TestServiceTracker.getInitOrder()).containsExactly("eagerService");
                        assertThat(TestServiceTracker.getStartOrder()).containsExactly("eagerService");

                        var lazyService = context.getBean("lazyService", TestService.class);
                        assertThat(lazyService).isNotNull();

                        assertThat(lazyService.getName()).isEqualTo("lazyService");
                        assertThat(TestServiceTracker.getInitOrder()).containsExactly("eagerService", "lazyService");
                        assertThat(TestServiceTracker.getStartOrder()).containsExactly("eagerService", "lazyService");
                    }
                );

            assertThat(TestServiceTracker.getStopOrder()).containsExactly("lazyService", "eagerService");
            assertThat(TestServiceTracker.getCleanupOrder()).containsExactly("lazyService", "eagerService");
        }
    }

    @Test
    void shouldNotHandlePrototypeBean() {
        contextRunner
            .withUserConfiguration(WithPrototypeServicesConfiguration.class)
            .run(context -> {
                    // Verify context started successfully
                    assertThat(context)
                        .hasNotFailed()
                        .hasBean("distributedTaskLifecycleSpringInitializer")
                        .hasBean("singletonService");

                    var prototypeService = context.getBean("prototypeService", TestService.class);
                    assertThat(prototypeService).isNotNull();

                    assertThat(prototypeService.getName()).isEqualTo("prototypeService");
                    assertThat(TestServiceTracker.getInitOrder()).containsExactly("singletonService");
                    assertThat(TestServiceTracker.getStartOrder()).containsExactly("singletonService");
                }
            );

        assertThat(TestServiceTracker.getStopOrder()).containsExactly("singletonService");
        assertThat(TestServiceTracker.getCleanupOrder()).containsExactly("singletonService");
    }

    //todo
    @Test
    void shouldNotStratContextWhenErrorInInitMethod() {

    }

    //todo
    void shouldNotStartContextWhenErrorInStartMethod() {

    }

    @Configuration
    static class TestServicesConfiguration {

        @Bean
        public TestService serviceA() {
            return new TestService("serviceA");
        }

        @Bean
        public TestService serviceB() {
            return new TestService("serviceB");
        }

        @Bean
        public TestService serviceC() {
            return new TestService("serviceC");
        }
    }

    @Configuration
    static class WithPrototypeServicesConfiguration {

        @Bean
        public TestService singletonService() {
            return new TestService("singletonService");
        }

        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        @Bean
        public TestService prototypeService() {
            return new TestService("prototypeService");
        }
    }

    @Configuration
    static class LazyServiceConfiguration {

        @Bean
        public TestService eagerService() {
            return new TestService("eagerService");
        }

        @Lazy
        @Bean
        public TestService lazyService() {
            return new TestService("lazyService");
        }
    }

    // firstService --- depends on ---> secondService
    // stop order: firstService, secondService
    @Configuration
    static class LazyDependenciesInRuntime {

        @Bean
        public TestLazyService firstService(@Lazy TestService secondService) {
            return new TestLazyService("firstService", secondService, TestLazyService.Mode.IN_RUNTIME);
        }

        @Bean
        public TestService secondService() {
            return new TestService("secondService");
        }
    }

    @Configuration
    static class LazyDependentServiceInStartingContextInInitConfiguration {

        @Bean
        public TestLazyService eagerService(@Lazy TestService lazyService) {
            return new TestLazyService("eagerService", lazyService, TestLazyService.Mode.ON_INIT);
        }

        @Lazy
        @Bean
        public TestService lazyService() {
            return new TestService("lazyService");
        }
    }

    @Configuration
    static class LazyDependentServiceInStartingContextInStartConfiguration {

        @Bean
        public TestLazyService eagerService(@Lazy TestService lazyService) {
            return new TestLazyService("eagerService", lazyService, TestLazyService.Mode.ON_START);
        }

        @Lazy
        @Bean
        public TestService lazyService() {
            return new TestService("lazyService");
        }
    }

    // eagerService --- depends on ---> lazyService
    // stop order: eagerService, lazyService
    @Configuration
    static class LazyDependentServiceInRuntime {

        @Bean
        public TestLazyService eagerService(@Lazy TestService lazyService) {
            return new TestLazyService("eagerService", lazyService, TestLazyService.Mode.IN_RUNTIME);
        }

        @Lazy
        @Bean
        public TestService lazyService() {
            return new TestService("lazyService");
        }
    }

    // secondService --- depends on ---> firstService
    // stop order: secondService, firstService
    @Configuration
    static class LazyDependenciesWithCircleServiceInRuntime {

        @Bean
        public TestLazyService firstService(@Lazy TestService secondService) {
            return new TestLazyService("firstService", secondService, TestLazyService.Mode.IN_RUNTIME);
        }

        @Bean
        public TestService secondService(TestLazyService firstService) {
            return new TestService("secondService", firstService);
        }
    }

    @Configuration
    static class LazyDependenciesAndLazyBeanWithCircleServiceInRuntime {

        @Bean
        public TestLazyService firstService(@Lazy TestService secondService) {
            return new TestLazyService("firstService", secondService, TestLazyService.Mode.IN_RUNTIME);
        }

        @Lazy
        @Bean
        public TestService secondService(TestLazyService firstService) {
            return new TestService("secondService", firstService);
        }
    }

    @Configuration
    static class MixedServicesConfiguration {

        @Bean
        public TestService serviceA() {
            return new TestService("serviceA");
        }

        @Bean
        public RegularBean regularBean() {
            return new RegularBean();
        }
    }

    static class RegularBean {
        // Regular bean that doesn't implement DistributedTaskServiceLifecycle
    }

    /**
     * Test service that tracks lifecycle method calls.
     */
    @Getter
    static class TestService implements DistributedTaskServiceLifecycle {
        protected final String name;
        protected final TestLazyService testLazyService;

        public TestService(String name) {
            this.name = name;
            this.testLazyService = null;
        }

        public TestService(String name, TestLazyService testLazyService) {
            this.name = name;
            this.testLazyService = testLazyService;
        }

        @Override
        public void init() throws Exception {
            TestServiceTracker.trackInit(name);
        }

        @Override
        public void start() throws Exception {
            TestServiceTracker.trackStart(name);
        }

        @Override
        public void stop() throws Exception {
            TestServiceTracker.trackStop(name);
        }

        @Override
        public void cleanup() throws Exception {
            TestServiceTracker.trackCleanup(name);
        }

        @PostConstruct
        public void postConstruct() {
            TestServiceTracker.trackPostConstruct(name);
        }

        @PreDestroy
        public void preDestroy() {
            TestServiceTracker.trackPreDestroyOrder(name);
        }
    }

    @Getter
    static class TestLazyService extends TestService {
        private final TestService lazyService;
        private final Mode mode;

        public TestLazyService(String name, TestService lazyService, Mode mode) {
            super(name);
            this.lazyService = lazyService;
            this.mode = mode;
        }


        @Override
        public void init() throws Exception {
            TestServiceTracker.trackInit(name);
            if (mode.equals(Mode.ON_INIT)) {
                lazyService.getName(); // lazy init of bean
            }
        }

        @Override
        public void start() throws Exception {
            TestServiceTracker.trackStart(name);
            if (mode.equals(Mode.ON_START)) {
                lazyService.getName(); // lazy init of bean
            }
        }

        @Override
        public String getName() {
            if (mode.equals(Mode.IN_RUNTIME)) {
                lazyService.getName(); // lazy init of bean
            }
            return super.getName();
        }

        enum Mode {
            ON_INIT,
            ON_START,
            IN_RUNTIME
        }
    }

    /**
     * Thread-safe tracker for lifecycle method calls.
     */
    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    static class TestServiceTracker {
        static List<String> initOrder = Collections.synchronizedList(new ArrayList<>());
        static List<String> startOrder = Collections.synchronizedList(new ArrayList<>());
        static List<String> stopOrder = Collections.synchronizedList(new ArrayList<>());
        static List<String> cleanupOrder = Collections.synchronizedList(new ArrayList<>());
        static List<String> postConstructOrder = Collections.synchronizedList(new ArrayList<>());
        static List<String> preDestroyOrder = Collections.synchronizedList(new ArrayList<>());

        public static void reset() {
            initOrder.clear();
            startOrder.clear();
            stopOrder.clear();
            cleanupOrder.clear();
            postConstructOrder.clear();
            preDestroyOrder.clear();
        }

        public static void trackInit(String serviceName) {
            initOrder.add(serviceName);
        }

        public static void trackStart(String serviceName) {
            startOrder.add(serviceName);
        }

        public static void trackStop(String serviceName) {
            stopOrder.add(serviceName);
        }

        public static void trackCleanup(String serviceName) {
            cleanupOrder.add(serviceName);
        }

        public static void trackPostConstruct(String serviceName) {
            postConstructOrder.add(serviceName);
        }

        public static void trackPreDestroyOrder(String serviceName) {
            preDestroyOrder.add(serviceName);
        }

        public static List<String> getInitOrder() {
            return new ArrayList<>(initOrder);
        }

        public static List<String> getStartOrder() {
            return new ArrayList<>(startOrder);
        }

        public static List<String> getStopOrder() {
            return new ArrayList<>(stopOrder);
        }

        public static List<String> getCleanupOrder() {
            return new ArrayList<>(cleanupOrder);
        }

        public static List<String> getPostConstructOrder() {
            return new ArrayList<>(postConstructOrder);
        }

        public static List<String> getPreDestroyOrder() {
            return new ArrayList<>(preDestroyOrder);
        }
    }
}