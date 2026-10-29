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
import org.springframework.boot.context.annotation.UserConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DistributedTaskLifecycleSpringConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(UserConfigurations.of(DistributedTaskLifecycleSpringConfiguration.class));

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

                    verifyTheSameInitOrder("serviceA", "serviceB", "serviceC");
                }
            );

        verifyTheSameCleanupOrder("serviceC", "serviceB", "serviceA");
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
                }
            );

        verifyTheSameCleanupOrder("serviceA");
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
                }
            );
    }

    @Nested
    class LazyDependenciesAndCycles {

        @Test
        void shouldHandleIndependentLazyDependencyBeanInRuntime() {
            contextRunner
                .withUserConfiguration(LazyDependenciesInRuntime.class)
                .run(context -> {
                        // Verify context started successfully
                        assertThat(context)
                            .hasNotFailed()
                            .hasBean("firstService");

                        verifyTheSameInitOrder("firstService", "secondService");
                        var eagerService = context.getBean("firstService", TestService.class);
                        assertThat(eagerService).isNotNull();

                        assertThat(eagerService.getName()).isEqualTo("firstService");
                    }
                );

            verifyTheSameCleanupOrder("firstService", "secondService");
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

            verifyTheSameCleanupOrder("eagerService", "lazyService");
        }

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

                        verifyTheSameInitOrder("eagerService", "lazyService");
                    }
                );

            verifyTheSameCleanupOrder("eagerService", "lazyService");
        }

        @Test
        void shouldHandleLazyDependentServiceWhenContextIsStarted() {
            contextRunner
                .withUserConfiguration(LazyDependentServiceInRuntime.class)
                .run(context -> {
                        // Verify context started successfully
                        assertThat(context)
                            .hasNotFailed()
                            .hasBean("eagerService");

                        verifyTheSameInitOrder("eagerService");

                        var eagerService = context.getBean("eagerService", TestService.class);
                        assertThat(eagerService).isNotNull();

                        assertThat(eagerService.getName()).isEqualTo("eagerService");
                        verifyTheSameInitOrder("eagerService", "lazyService");
                    }
                );

            verifyTheSameCleanupOrder("eagerService", "lazyService");
        }

        // todo: there is a problem: for circle dependencies spring doesn't make difference which dependency is first,
        // which a second! But we in dtf configuration does! Now: we just do the same as spring do.
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

                        verifyTheSameInitOrder("firstService", "secondService");
                        var eagerService = context.getBean("firstService", TestService.class);
                        assertThat(eagerService).isNotNull();

                        assertThat(eagerService.getName()).isEqualTo("firstService");
                    }
                );

            verifyTheSameCleanupOrder(
                // "secondService", "firstService" // desirable, but spring don't provide it for us
                "firstService", "secondService"
            );
        }

        // todo: see shouldHandleLazyDependenciesWithCircleBeanWhenContextIsStarted
        @Test
        void shouldHandleLazyDependenciesAndLazyBeanWithCircleBeanWhenContextIsStarted() {
            contextRunner
                .withUserConfiguration(LazyDependenciesAndLazyBeanWithCircleServiceInRuntime.class)
                .run(context -> {
                        // Verify context started successfully
                        assertThat(context)
                            .hasNotFailed()
                            .hasBean("firstService");

                        verifyTheSameInitOrder("firstService");
                        var eagerService = context.getBean("firstService", TestService.class);
                        assertThat(eagerService).isNotNull();

                        assertThat(eagerService.getName()).isEqualTo("firstService");
                    }
                );

            verifyTheSameCleanupOrder(
                // "secondService", "firstService" // desirable, but spring don't provide it for us
                "firstService", "secondService"
            );
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

                        verifyTheSameInitOrder("eagerService");
                        var lazyService = context.getBean("lazyService", TestService.class);
                        assertThat(lazyService).isNotNull();

                        assertThat(lazyService.getName()).isEqualTo("lazyService");
                        verifyTheSameInitOrder("eagerService", "lazyService");
                    }
                );

            verifyTheSameCleanupOrder("lazyService", "eagerService");
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

        verifyTheSameCleanupOrder("singletonService");
    }

    @Test
    void shouldHandleTriangleDependencies() {
        contextRunner
            .withUserConfiguration(TriangleDependenciesConfiguration.class)
            .run(context -> {
                    // Verify context started successfully
                    assertThat(context)
                        .hasNotFailed()
                        .hasBean("firstService")
                        .hasBean("secondService")
                        .hasBean("dependentService")
                    ;
                    verifyTheSameInitOrder("firstService", "secondService", "dependentService");
                }
            );

        verifyTheSameCleanupOrder("dependentService", "secondService", "firstService");
    }

    //todo
    @Test
    void shouldNotStartContextWhenErrorInInitMethod() {

    }

    //todo
    void shouldNotStartContextWhenErrorInStartMethod() {

    }

    private void verifyTheSameInitOrder(String... serviceNames) {
        assertThat(TestServiceTracker.getPostConstructOrder()).containsExactly(serviceNames);
        assertThat(TestServiceTracker.getInitOrder()).containsExactlyElementsOf(TestServiceTracker.getPostConstructOrder());
        assertThat(TestServiceTracker.getStartOrder()).containsExactlyElementsOf(TestServiceTracker.getPostConstructOrder());
    }

    private void verifyTheSameCleanupOrder(String... serviceNames) {
        assertThat(TestServiceTracker.getPreDestroyOrder()).containsExactly(serviceNames);
        assertThat(TestServiceTracker.getStopOrder()).containsExactlyElementsOf(TestServiceTracker.getPreDestroyOrder());
        assertThat(TestServiceTracker.getCleanupOrder()).containsExactlyElementsOf(TestServiceTracker.getPreDestroyOrder());
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

    @Configuration
    static class LazyDependenciesInRuntime {

        @Bean
        public RunLazyService firstService(@Lazy TestService secondService) {
            return new RunLazyService("firstService", secondService, RunLazyService.Mode.IN_RUNTIME);
        }

        @Bean
        public TestService secondService() {
            return new TestService("secondService");
        }
    }

    @Configuration
    static class LazyDependentServiceInStartingContextInInitConfiguration {

        @Bean
        public RunLazyService eagerService(@Lazy TestService lazyService) {
            return new RunLazyService("eagerService", lazyService, RunLazyService.Mode.ON_INIT);
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
        public RunLazyService eagerService(@Lazy TestService lazyService) {
            return new RunLazyService("eagerService", lazyService, RunLazyService.Mode.ON_START);
        }

        @Lazy
        @Bean
        public TestService lazyService() {
            return new TestService("lazyService");
        }
    }

    @Configuration
    static class LazyDependentServiceInRuntime {

        @Bean
        public RunLazyService eagerService(@Lazy TestService lazyService) {
            return new RunLazyService("eagerService", lazyService, RunLazyService.Mode.IN_RUNTIME);
        }

        @Lazy
        @Bean
        public TestService lazyService() {
            return new TestService("lazyService");
        }
    }

    @Configuration
    static class LazyDependenciesWithCircleServiceInRuntime {

        @Bean
        public RunLazyService firstService(@Lazy TestService secondService) {
            return new RunLazyService("firstService", secondService, RunLazyService.Mode.IN_RUNTIME);
        }

        @Bean
        public TestService secondService(RunLazyService firstService) {
            return new TestService("secondService", firstService);
        }
    }

    @Configuration
    static class LazyDependenciesAndLazyBeanWithCircleServiceInRuntime {

        @Bean
        public RunLazyService firstService(@Lazy TestService secondService) {
            return new RunLazyService("firstService", secondService, RunLazyService.Mode.IN_RUNTIME);
        }

        @Lazy
        @Bean
        public TestService secondService(RunLazyService firstService) {
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

    @Configuration
    static class TriangleDependenciesConfiguration {

        @Bean
        public TestService firstService() {
            return new TestService("firstService");
        }

        @Bean
        public TestService dependentService(TestService firstService, TestService secondService) {
            return new DependentTestService("dependentService", List.of(firstService, secondService));
        }

        @Bean
        public TestService secondService(TestService firstService) {
            return new DependentTestService("secondService", List.of(firstService));
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
        protected final RunLazyService runLazyService;

        public TestService(String name) {
            this.name = name;
            this.runLazyService = null;
        }

        public TestService(String name, RunLazyService runLazyService) {
            this.name = name;
            this.runLazyService = runLazyService;
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
    static class DependentTestService extends TestService {
        private final List<TestService> dependencies;

        public DependentTestService(String name, List<TestService> dependencies) {
            super(name);
            this.dependencies = dependencies;
        }
    }

    @Getter
    static class RunLazyService extends TestService {
        private final TestService lazyService;
        private final Mode mode;

        public RunLazyService(String name, TestService lazyService, Mode mode) {
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