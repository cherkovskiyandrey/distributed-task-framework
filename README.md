# Distributed Task Framework

[![Maven Central](https://img.shields.io/maven-central/v/io.github.cherkovskiyandrey/distributed-task-spring-boot-starter.svg?label=Maven%20Central)](https://central.sonatype.com/search?q=g:io.github.cherkovskiyandrey)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A lightweight task processor for JVM microservices that needs nothing but your PostgreSQL database. Supports
horizontal scaling, affinity protection, map-reduce style join tasks, stateful tasks, cron tasks and
exactly-once execution for database work. On top of it, a saga framework for orchestrating distributed
transactions with compensation.

<!-- TOC -->
* [Distributed Task Framework](#distributed-task-framework)
  * [What is in the box](#what-is-in-the-box)
  * [Why](#why)
  * [A taste](#a-taste)
    * [A task](#a-task)
    * [A saga](#a-saga)
  * [Getting started](#getting-started)
  * [Modules and artifacts](#modules-and-artifacts)
  * [Repository layout](#repository-layout)
  * [Versions and compatibility](#versions-and-compatibility)
  * [Building and running the examples](#building-and-running-the-examples)
  * [Documentation](#documentation)
  * [License](#license)
  * [Disclaimer](#disclaimer)
<!-- TOC -->

## What is in the box

| Part | What it does | Docs |
|---|---|---|
| **DTF, Distributed Task Framework** | Schedules and executes typed tasks across all instances of an application. A planner elected through the database assigns work to workers; retries, timeouts, affinity, join tasks, cron tasks and dead letters are built in. | [core/README.md](core/README.md) |
| **DSF, Distributed Saga Framework** | Builds sagas (chains of business methods with compensating methods) on DTF: `create(...).registerToRun(...).thenRun(...).start()`. Persistence, retries, cancellation, expiration and versioning for rolling deployments included. | [saga/README.md](saga/README.md) |
| **Test utilities** | Spring Boot test starters with fast settings, a mocked cluster, cron tasks on demand and `reinitAndWait()` helpers to reset DTF / DSF state between tests. | [test-utils/dtf/README.md](test-utils/dtf/README.md), [test-utils/dsf/README.md](test-utils/dsf/README.md) |
| **Examples** | Two runnable Spring Boot applications exercising the DTF and the saga API through REST. | [examples/](examples) |
| **Perf tests** | A load-test application that generates a fan-out / fan-in DAG of tasks and checks delivery correctness under load. | [tests/README.md](tests/README.md) |

## Why

- **No extra infrastructure.** All state and all node-to-node communication live in the database you already run.
  No broker, no coordinator, no sidecar.
- **Transactional scheduling.** A task is inserted in the same transaction as the business row that caused it, so
  there is no "row saved but task lost" window. Tasks that touch only the database can run exactly once.
- **Horizontal scaling for free.** Every instance is a worker; one of them is the planner and fails over through
  the database when it disappears.
- **Ordering where you need it, parallelism everywhere else.** Affinity serializes tasks that work on the same
  resource; everything else runs concurrently under per-node and per-cluster limits.
- **Composable workflows.** Child tasks, join tasks for map/reduce, stateful retries, and sagas for cross-service
  consistency, all with the same typed API.

## A taste

### A task

```java
@Component
public class HelloTask implements Task<HelloMessage> {
    public static final TaskDef<HelloMessage> DEF = TaskDef.privateTaskDef("HELLO_TASK", HelloMessage.class);

    @Override
    public TaskDef<HelloMessage> getDef() {
        return DEF;
    }

    @Override
    public void execute(ExecutionContext<HelloMessage> ctx) {
        System.out.println(ctx.getInputMessageOrThrow().helloText());
    }
}
```

```java
TaskId taskId = distributedTaskService.schedule(HelloTask.DEF, ExecutionContext.simple(new HelloMessage("hi")));
```

### A saga

```java
public SagaFlow<OrderConfirmation> placeOrder(OrderRequest request) {
    return sagaService
        .createWithAffinity("place-order", "ORDERS", request.orderId().toString())
        .registerToRun(this::createOrder, this::deleteOrder, request)   // step + compensation
        .thenRun(this::chargePayment, this::refundPayment)
        .thenRun(this::confirmOrder)                                   // nothing to compensate
        .start();
}
```

```java
OrderConfirmation result = placeOrder(request).get(Duration.ofMinutes(1)).orElseThrow();
```

## Getting started

1. Add the starter (and a PostgreSQL driver plus Liquibase, which are not transitive):

   ```groovy
   implementation 'io.github.cherkovskiyandrey:distributed-task-spring-boot-starter:2.0.0-rc.2'
   // for sagas instead (brings the DTF starter transitively):
   // implementation 'io.github.cherkovskiyandrey:distributed-task-base-saga-spring-boot-starter:2.0.0-rc.2'
   runtimeOnly 'org.postgresql:postgresql'
   implementation 'org.liquibase:liquibase-core'
   ```

2. Include the DTF (and, for sagas, the DSF) changelog in your Liquibase master:

   ```yaml
   databaseChangeLog:
     - include:
         file: db/changelog/distributed-task-framework/db.changelog-aggregator.yaml
     - include:
         file: db/changelog/distributed-task-base-saga/db.changelog-aggregator.yaml   # sagas only
   ```

3. Switch it on:

   ```yaml
   distributed-task:
     enabled: true
     common:
       app-name: 'my-app'
     saga:
       enabled: true   # sagas only
   ```

4. Follow the quick start in [core/README.md](core/README.md#quick-start) or
   [saga/README.md](saga/README.md#quick-start).

## Modules and artifacts

All artifacts are published to Maven Central under the group `io.github.cherkovskiyandrey`; the artifact id equals
the module name.

| Module | Role | Add to your project? |
|---|---|---|
| `distributed-task-spring-boot-starter` | DTF for Spring Boot | **yes**, for tasks |
| `distributed-task-spring-boot-autoconfigure` | DTF autoconfiguration, properties, annotations | via the starter |
| `distributed-task-library` | DTF engine (planner, workers, repositories, Liquibase changelogs) | via the starter |
| `distributed-task-base-saga-spring-boot-starter` | DSF for Spring Boot (includes the DTF starter) | **yes**, for sagas |
| `distributed-task-base-saga-spring-boot-autoconfigure` | DSF autoconfiguration, properties, `@SagaMethod` / `@SagaRevertMethod` | via the starter |
| `distributed-task-base-saga-library` | DSF engine and Liquibase changelog | via the starter |
| `distributed-task-test-spring-boot-starter` | DTF test support | **yes**, `testImplementation` |
| `distributed-task-test-spring-boot-autoconfigure` | test autoconfiguration, `DistributedTaskTestUtil` | via the test starter |
| `distributed-task-test` | mocked `ClusterProvider` | via the test starter |
| `distributed-task-base-saga-test-spring-boot-starter` | DSF test support (includes the DTF test starter) | **yes**, `testImplementation` |
| `distributed-task-base-saga-test-spring-boot-autoconfigure` | `SagaTestUtil` | via the test starter |
| `common-utils`, `common-spring-utils` | shared internals: lifecycle, caches, metrics helper, `DtfJdbcInfrastructure` | transitively |
| `distributed-task-perf-test-*` | load-test library, autoconfiguration and starter | no, internal |

## Repository layout

```
commons/      shared utilities (lifecycle, caches, metrics helper, JDBC infrastructure)
core/         DTF: library, autoconfigure, starter               -> core/README.md
saga/         DSF: library, autoconfigure, starter               -> saga/README.md
test-utils/   test starters for DTF (dtf/) and DSF (dsf/)        -> test-utils/*/README.md
examples/     runnable example applications                      -> examples/*/README.MD
tests/        performance test library and application          -> tests/README.md
```

## Versions and compatibility

| Requirement | Value |
|---|---|
| Java | 17+ |
| Spring Boot | 3.3.x |
| Database | PostgreSQL |

| Version | Notes |
|---|---|
| `2.0.0-rc.2` | current. Unified service lifecycle, pluggable `DtfJdbcInfrastructure`, Liquibase migration check, planner fixes for large tables, saga fixes. Requires schema migration 005. Breaking: `@DtfDataSource` and `DtfRepositoryConstants` removed, see [Migration from 1.x](core/README.md#migration-from-1x). |
| `2.0.0-rc.1` | first release with the saga framework. |
| `1.0.0` | first stable DTF release. |

Release notes: [GitHub releases](https://github.com/cherkovskiyandrey/distributed-task-framework/releases).

## Building and running the examples

```bash
./gradlew build                                                   # compile and run all tests (needs Docker for Testcontainers)
docker compose up -d                                              # PostgreSQL 16 on localhost:5432 for the example apps
./gradlew :distributed-task-test-application:bootRun              # DTF example, Swagger UI at http://localhost:8080/swagger-ui.html
./gradlew :distributed-task-base-saga-test-application:bootRun    # saga example, Swagger UI at http://localhost:8080/swagger-ui/index.html
./gradlew :distributed-task-perf-test-application:bootRun         # perf test application
```

Integration tests start their own PostgreSQL through Testcontainers, so a running Docker daemon is the only
prerequisite for `./gradlew build`.

## Documentation

- [DTF documentation](core/README.md): install, quick start, every concept, configuration, internals, metrics,
  troubleshooting, migration from 1.x.
- [DTF configuration properties](core/distributed-task-spring-boot-autoconfigure/config-prop.md) (generated).
- [DSF documentation](saga/README.md): install, quick start, saga and revert methods, flow builder, failure and
  compensation, cancellation, timeouts, versioning, configuration, metrics.
- [DSF configuration properties](saga/distributed-task-base-saga-spring-boot-autoconfigure/config-prop.md) (generated).
- [DTF test utilities](test-utils/dtf/README.md) and [DSF test utilities](test-utils/dsf/README.md).
- [Example applications](examples) and [performance tests](tests/README.md).

Issues and discussions: [GitHub](https://github.com/cherkovskiyandrey/distributed-task-framework/issues).

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

## Disclaimer

All information and source code are provided AS-IS, without express or implied warranties.
Use of the source code or parts of it is at your sole discretion and risk.
Cherkovskiy Andrey takes reasonable measures to ensure the relevance of the information posted in this repository,
but does not assume responsibility for maintaining or updating this repository or its parts outside the framework
established independently and without notifying third parties.
