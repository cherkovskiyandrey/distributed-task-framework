# Performance tests

Load test for the [Distributed Task Framework](../core/README.md). A run generates a configurable fan-out /
fan-in DAG of tasks, spreads it over several affinities and verifies that every join task received exactly the
messages it should have. The result is a pass/fail verdict per pipeline plus the wall-clock duration of the run;
throughput is read from DTF's Micrometer metrics.

## Modules

| Module | Role |
|---|---|
| `distributed-task-perf-test-library` | tasks, Spring Data JDBC entities and repositories, `PerfTestService`, REST controller, Liquibase changelog for the result tables |
| `distributed-task-perf-test-spring-boot-autoconfigure` | autoconfiguration of the library |
| `distributed-task-perf-test-spring-boot-starter` | starter for the two modules above |
| `distributed-task-perf-test-application` | runnable Spring Boot application with Swagger UI and a Prometheus endpoint |

## The DAG

```
GENERATED_TASK
  -> PARENT_TASK                 x totalPipelines           (exactly-once)
      -> CHILD_TASK_LEVEL_ONE    x totalTaskOnFirstLevel    (at-least-once)
          -> CHILD_TASK_LEVEL_TWO  x totalTaskOnSecondLevel (at-least-once)
          -> JOIN_TASK_LEVEL_TWO                            (exactly-once, sums the children's numbers)
      -> JOIN_TASK_LEVEL_ONE                                (exactly-once, verifies the whole tree)
```

Every task writes an intermediate result row; join tasks receive their inputs through
`getJoinMessagesFromBranch` / `setJoinMessageToBranch`. `JOIN_TASK_LEVEL_ONE` recomputes what the tree should
have produced and marks the pipeline `DONE` or `FAILED` (with one `dtf_failed_task_result` row per mismatch).
Pipelines are distributed over `totalAffinities` affinities inside one affinity group, so the run also exercises
affinity serialization.

Total tasks per run: `totalPipelines * (1 + L1 + L1 * L2 + L1 + 1) + 1`, where `L1 = totalTaskOnFirstLevel` and
`L2 = totalTaskOnSecondLevel`.

## Run

```bash
docker compose up -d                                        # from the repository root
./gradlew :distributed-task-perf-test-application:bootRun
```

The data source is taken from `PG_MASTER_HOST`, `PG_MASTER_PORT`, `PG_DB_NAME`, `PG_USERNAME`, `PG_PASSWORD`
(defaults match the compose file). Swagger UI: <http://localhost:8080/swagger-ui/index.html>; Prometheus
metrics: <http://localhost:8080/metrics>; health: <http://localhost:8080/health/check>.

Start a run:

```bash
curl -X POST http://localhost:8080/api/dtf/perf-test \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "run-1",
        "affinityGroup": "perf",
        "totalPipelines": 100,
        "totalAffinities": 10,
        "totalTaskOnFirstLevel": 10,
        "totalTaskOnSecondLevel": 10,
        "taskDurationMs": 1
      }'
```

Poll until every pipeline is `DONE`:

```bash
curl http://localhost:8080/api/dtf/perf-test/run-1
```

The response contains `createdAt`, `completedAt`, `duration` and `summaryStates` (`NEW`, `IN_PROGRESS`, `DONE`,
`FAILED`). `DELETE /api/dtf/perf-test/{name}` removes a run and its results. Run several instances of the
application against the same database to test cluster behaviour.

`taskDurationMs` is accepted and stored but currently has no effect (the sleep in the child tasks is commented
out).

DTF tuning used for the published numbers lives in
`distributed-task-perf-test-application/src/main/resources/application.yml` (batch sizes 300, polling and manage
delay 10 ms under load, 200 parallel tasks per node).

## Smoke test

`PerfTestServiceIntegrationTest` in the library runs a tiny configuration (2 pipelines, 1 x 1) against a
Testcontainers PostgreSQL and asserts that every pipeline ends `DONE`:

```bash
./gradlew :distributed-task-perf-test-library:test
```

## Results

One node, Apple M2 Pro, 32 GB RAM, local PostgreSQL:

| Nodes | max-parallel-tasks-in-cluster-default | batch-size | polling-delay | new-batch-size | deleted-batch-size | max-parallel-tasks-in-node | manage-delay | Total tasks | Total time | RPM | RPS |
|-------|---------------------------------------|------------|---------------|----------------|--------------------|----------------------------|--------------|-------------|------------|-----|-----|
| 1 | 300 | 300 | 10 ms | 300 | 300 | 200 | 10 ms | 12_021 | 43 sec | **16744** | **279** |
| 1 | 300 | 300 | 10 ms | 300 | 300 | 200 | 10 ms | 120_201 | 382 sec | **18480** | **314** |
