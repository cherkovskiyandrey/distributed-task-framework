# DTF test utilities

Spring Boot test support for applications built on the
[Distributed Task Framework](../../core/README.md): fast planner and worker settings, a mocked cluster provider,
on-demand cron tasks and a `DistributedTaskTestUtil` bean that resets DTF state between tests.

<!-- TOC -->
* [DTF test utilities](#dtf-test-utilities)
  * [Modules](#modules)
  * [Install](#install)
  * [Activation](#activation)
  * [Quick start](#quick-start)
  * [What the test autoconfiguration changes](#what-the-test-autoconfiguration-changes)
    * [Fast defaults](#fast-defaults)
    * [Mocked cluster provider](#mocked-cluster-provider)
    * [Cron tasks on demand](#cron-tasks-on-demand)
  * [DistributedTaskTestUtil](#distributedtasktestutil)
  * [Recipes](#recipes)
    * [Database for tests](#database-for-tests)
    * [Waiting for a task](#waiting-for-a-task)
    * [Testing a cron task](#testing-a-cron-task)
    * [Testing retries and the dead letter table](#testing-retries-and-the-dead-letter-table)
    * [Keeping a long-running task alive across resets](#keeping-a-long-running-task-alive-across-resets)
  * [License](#license)
<!-- TOC -->

## Modules

| Module | Artifact | Contents |
|---|---|---|
| `distributed-task-test` | `io.github.cherkovskiyandrey:distributed-task-test` | `ClusterProviderTestImpl`, a `ClusterProvider` replacement that reports the node as registered immediately and never overloaded. No Spring Boot dependency. |
| `distributed-task-test-spring-boot-autoconfigure` | `io.github.cherkovskiyandrey:distributed-task-test-spring-boot-autoconfigure` | `TestDistributedTaskAutoconfiguration`, `DistributedTaskTestUtil`, `application-dtf-test-utils.yml` with fast defaults. |
| `distributed-task-test-spring-boot-starter` | `io.github.cherkovskiyandrey:distributed-task-test-spring-boot-starter` | Starter: autoconfigure module + `spring-boot-starter`. This is what you add to a project. |

## Install

```groovy
testImplementation 'io.github.cherkovskiyandrey:distributed-task-test-spring-boot-starter:2.0.0-rc.2'
```

```xml
<dependency>
    <groupId>io.github.cherkovskiyandrey</groupId>
    <artifactId>distributed-task-test-spring-boot-starter</artifactId>
    <version>2.0.0-rc.2</version>
    <scope>test</scope>
</dependency>
```

The starter does not bring a database driver, Liquibase or Testcontainers; your test classpath needs them the
same way your production classpath does.

## Activation

`TestDistributedTaskAutoconfiguration` is registered through `AutoConfiguration.imports` and activates only when
**all** of the following hold:

1. the Spring profile `test` is active (`@ActiveProfiles("test")`);
2. `distributed-task.enabled=true`;
3. the DTF starter is on the classpath.

Without the `test` profile the production autoconfiguration runs unchanged, so adding the starter to a project
cannot accidentally affect non-test contexts.

## Quick start

```java
@ActiveProfiles("test")
@SpringBootTest(properties = {
    "distributed-task.enabled=true",
    "distributed-task.common.app-name=test"
})
class HelloTaskTest {

    @Autowired
    DistributedTaskService distributedTaskService;
    @Autowired
    DistributedTaskTestUtil distributedTaskTestUtil;
    @Autowired
    GreetingRepository greetingRepository;

    @BeforeEach
    void reset() throws Exception {
        // cancel everything left by previous tests and wait until DTF is idle
        distributedTaskTestUtil.reinitAndWait();
    }

    @Test
    void shouldStoreGreeting() throws Exception {
        TaskId taskId = distributedTaskService.schedule(
            HelloTask.HELLO_TASK_DEF,
            ExecutionContext.simple(new HelloMessage("hi"))
        );

        distributedTaskService.waitCompletionAllWorkflow(taskId, Duration.ofSeconds(10));

        assertThat(greetingRepository.findAll()).extracting(Greeting::text).containsExactly("hi");
    }
}
```

With the fast defaults a scheduled task usually starts within a few tens of milliseconds, so tests stay quick.

## What the test autoconfiguration changes

The test autoconfiguration runs before `DistributedTaskAutoConfiguration` and relies on the fact that every
production bean is `@ConditionalOnMissingBean`. It replaces exactly four beans.

### Fast defaults

`CommonSettings` is built as: code defaults, overridden by `application-dtf-test-utils.yml` from the module,
overridden by your own `distributed-task.common.*` properties. The shipped file is not a Spring profile and does
not need to be included anywhere. Its values:

```yaml
distributed-task:
  common:
    completion:
      handler-initial-delay: PT0.01S
      handler-fixed-delay: PT0.01S
    registry:
      update-initial-delay-ms: 0
      update-fixed-delay-ms: 100
      max-inactivity-interval-ms: 1000
      cache-expiration-ms: 100
    planner:
      watchdog-initial-delay-ms: 0
      watchdog-fixed-delay-ms: 100
      max-parallel-tasks-in-cluster-default: 10
      batch-size: 10
      polling-delay:
        0: 10
        1: 10
      new-batch-size: 100
      deleted-batch-size: 100
    worker-manager:
      max-parallel-tasks-in-node: 10
      manage-delay:
        0: 10
        1: 10
```

Note the small parallelism limits (10 tasks in the cluster and per node). Raise them in your test properties if a
test schedules many tasks at once.

### Mocked cluster provider

`ClusterProviderTestImpl` replaces the heartbeat-based `ClusterProvider`. The node is considered registered from
the first moment, never excluded from planning as overloaded, and supports every planner capability. Tests do
not wait for the first heartbeat and do not depend on CPU load of the CI machine.

### Cron tasks on demand

Cron tasks are registered **without** their cron expression (the setting is stripped at registration time), so
they are not scheduled on startup and do not fire in the background. Instead you schedule them like any other
task when the test needs them. To keep production behaviour, set:

```yaml
distributed-task:
  test:
    cron:
      enabled: true
```

## DistributedTaskTestUtil

```java
public interface DistributedTaskTestUtil {
    void reinitAndWait();
    void reinitAndWait(List<TaskDef<?>> excludeList);
    void reinitAndWait(int attemptsToCancel, Duration duration);
    void reinitAndWait(int attemptsToCancel, Duration duration, List<TaskDef<?>> excludeList);
}
```

`reinitAndWait` brings DTF back to an idle state:

1. cancels all workflows of all tasks not in `excludeList` (repeating up to `attemptsToCancel` times while new
   tasks keep appearing);
2. waits up to `duration` for every cancelled workflow to complete;
3. waits until the worker manager runs nothing but excluded tasks and the task table contains nothing but excluded
   tasks (completed rows are purged by the planner);
4. clears the dead letter table.

Defaults are 10 attempts and 1 minute. `FailedCancellationException` (unchecked) is thrown when tasks could not
be cancelled or did not finish in time; `InterruptedException` propagates from waiting.

Call it in `@BeforeEach`. Calling it in `@AfterEach` as well is cheap and leaves the database clean for the next
test class.

## Recipes

### Database for tests

DTF needs a real PostgreSQL. The project itself uses Testcontainers; a minimal initializer looks like this:

```java
public class PostgresInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @Override
    public void initialize(ConfigurableApplicationContext ctx) {
        POSTGRES.start();
        TestPropertyValues.of(
            "spring.datasource.url=" + POSTGRES.getJdbcUrl(),
            "spring.datasource.username=" + POSTGRES.getUsername(),
            "spring.datasource.password=" + POSTGRES.getPassword()
        ).applyTo(ctx.getEnvironment());
    }
}
```

```java
@ContextConfiguration(initializers = PostgresInitializer.class)
```

Run the DTF Liquibase changelog in tests exactly as in production (include
`db/changelog/distributed-task-framework/db.changelog-aggregator.yaml` in the test master changelog); the
migration check is active in tests too.

### Waiting for a task

Use the DTF API rather than `Thread.sleep`:

```java
TaskId taskId = distributedTaskService.schedule(MY_TASK, ExecutionContext.simple(msg));
distributedTaskService.waitCompletion(taskId, Duration.ofSeconds(10));            // this task only
distributedTaskService.waitCompletionAllWorkflow(taskId, Duration.ofSeconds(10)); // the task and everything it spawned
```

For side effects that appear asynchronously (rows written by child tasks, messages sent), combine with Awaitility.

### Testing a cron task

Because cron is stripped in tests, a cron task is just a task:

```java
@Test
void shouldRunNightlyReport() throws Exception {
    TaskId taskId = distributedTaskService.schedule(NightlyReportTask.DEF, ExecutionContext.empty());
    distributedTaskService.waitCompletion(taskId, Duration.ofSeconds(10));
    assertThat(reportRepository.count()).isEqualTo(1);
}
```

### Testing retries and the dead letter table

Give the task under test a short retry policy in the test properties:

```yaml
distributed-task:
  task-properties-group:
    task-properties:
      FLAKY_TASK:
        retry:
          retry-mode: FIXED
          fixed:
            delay: PT0.1S
            max-number: 3
```

Then schedule it, wait for completion (a task that ends in the dead letter table also counts as completed) and
inspect `distributedTaskService.getDltTasksByDef(FLAKY_TASK, Pageable.unpaged())`.

### Keeping a long-running task alive across resets

If a test setup needs a task that must survive `reinitAndWait` (for example an internal housekeeping task),
exclude it:

```java
distributedTaskTestUtil.reinitAndWait(List.of(HousekeepingTask.DEF));
```

This is exactly what the saga test utilities do for the internal saga manager task.

## License

Copyright 2022 Cherkovskiy Andrey

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
