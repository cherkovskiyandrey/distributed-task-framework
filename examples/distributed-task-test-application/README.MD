# DTF example application

A runnable Spring Boot application that exercises the [Distributed Task Framework](../../core/README.md) API
through a REST controller: simple tasks, a cron task, a map/reduce flow, a multilevel join-task DAG, a task
timeout and an out-of-memory task for resiliency experiments.

## Run

```bash
docker compose up -d                                   # from the repository root: PostgreSQL 16 on localhost:5432
./gradlew :distributed-task-test-application:bootRun
```

Swagger UI: <http://localhost:8080/swagger-ui.html>. The Liquibase changelog of the application includes the DTF
aggregator, so the schema is created on first start. Start a second instance on another port
(`--server.port=8081`) to watch tasks being spread across two nodes.

## Endpoints

All endpoints are under `/api/test-task`.

| Method and path | What it shows |
|---|---|
| `POST /api/test-task` | schedules `SIMPLE_CONSOLE_OUTPUT` (exactly-once), which schedules a child task in the same workflow |
| `POST /api/test-task/parent-for-map-reduce-dag` | schedules `PARENT_TASK`: 10 children, each with 10 grandchildren, two levels of join tasks and a forked branch outside the join hierarchy |
| `POST /api/test-task/count-words` | map/reduce: splits the text into partitions, counts words in `MAP_TASK`s and prints the top 10 from `REDUCE_TASK` |
| `POST /api/test-task/check-timeout` | `CHECK_TIMEOUT_TASK` sleeps longer than its `timeout: PT10S` and gets interrupted |
| `POST /api/test-task/oom` | `TEST_OOM_TASK` allocates 2 GB; run the application with `-Xmx100m` to see how a crashing node is handled |

## What to look at

| Class | Demonstrates |
|---|---|
| `tasks/SimpleConsoleOutputTask` | `@TaskExecutionGuarantees(EXACTLY_ONCE)`, scheduling a child with `ctx.withNewMessage(...)` |
| `tasks/SimpleCronTask` | a cron task (the annotation is commented out; enable `@TaskSchedule` or the `cron` property to see it fire) |
| `tasks/ParentTask`, `ChildTaskLevelOne`, `ChildTaskLevelTwo`, `JoinTaskLevelTwo`, `JoinTask` | `scheduleJoin`, `getJoinMessagesFromBranch` / `setJoinMessageToBranch`, nested join tasks |
| `tasks/ChildTaskLevelTwo` | `scheduleFork` to start a branch that the join tasks do not wait for |
| `tasks/mapreduce/*` | a complete map/reduce flow |
| `tasks/CheckTimeoutTask` | reacting to a timeout in `onFailureWithResult` |
| `tasks/PrivateTaskDefinitions` | all `TaskDef`s in one place, with an ASCII diagram of the DAG |
| `resources/application.yml` | DTF configuration: planner and worker tuning, per-task retry, timeout and execution guarantee |
| `resources/db/changelog/db.changelog-master.yaml` | how to include the DTF Liquibase aggregator |

The `delivery-manager.remote-apps` entries in `application.yml` point to hosts that do not exist; they only show
how remote applications would be configured for the experimental remote task feature.
