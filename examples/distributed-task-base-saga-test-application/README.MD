# Saga example application

A runnable Spring Boot application (Java and Kotlin services) that exercises the
[Distributed Saga Framework](../../saga/README.md) through a REST controller: a four-step saga with compensation,
run synchronously or asynchronously, polled by track id, and cancelled gracefully or forcibly.

## Run

```bash
docker compose up -d                                             # from the repository root: PostgreSQL 16 on localhost:5432
./gradlew :distributed-task-base-saga-test-application:bootRun
```

Swagger UI: <http://localhost:8080/swagger-ui/index.html>. The Liquibase master includes the DTF aggregator, the
DSF aggregator and the application's own tables (`test_data`, `audit`).

## The saga

`services/impl/TestSagaServiceImpl` builds this chain for every request:

```
createLocal        -> createOnRemoteServiceOne (v2) -> createOnRemoteServiceTwo -> saveAudit
   | deleteLocal         | deleteOnRemoteServiceOne       | deleteOnRemoteServiceTwo    (no revert)
```

- `createLocal` is `@Transactional` (so it runs exactly-once) and lists `OptimisticLockingFailureException` and
  `DuplicateKeyException` in `noRetryFor`.
- `createOnRemoteServiceOne` exists twice: the deprecated version 0 and the current version 2, showing how a method
  is evolved during a rolling deployment.
- The revert methods show the different parameter shapes: `deleteLocal(input, output, exception)` and
  `deleteOnRemoteServiceOne(parentOutput, rootInput, output, exception)`.
- The saga is created with `createWithAffinity("test", "TEST_DATA_MANAGEMENT", id)`, so two requests for the same
  `id` run one after another.
- `services/impl/TestKotlinServiceImpl.kt` is the same idea in Kotlin, without compensation.
- `naiveSagaCall` in the same class is the "before" picture: a plain `@Transactional` method calling two remote
  services, with comments on what goes wrong when a remote call fails after the local write.

## Endpoints

All endpoints are under `/api/saga`. The request body is a `TestDataDto`:

```json
{
  "id": 1,
  "version": 0,
  "remoteServiceOneId": "r1",
  "remoteOneData": "one",
  "remoteServiceTwoId": "r2",
  "remoteTwoData": "two",
  "throwExceptionOnLevel": 3,
  "levelToDelay": 2,
  "syntheticDelayOnEachLevel": "PT20S"
}
```

`throwExceptionOnLevel` (1..4) makes that step throw, which triggers compensation; `levelToDelay` together with
`syntheticDelayOnEachLevel` slows a step down so you have time to cancel.

| Method and path | What it does |
|---|---|
| `POST /api/saga/sync` | runs the saga and waits for completion |
| `POST /api/saga/sync/with-result` | runs the saga and returns the `Audit` produced by the last step |
| `POST /api/saga/sync/with-result-kotlin` | the Kotlin saga |
| `POST /api/saga/async` | starts the saga and returns its track id |
| `POST /api/saga/async/without-track-id` | fire and forget |
| `GET /api/saga/{trackId}` | returns the result when completed, empty otherwise |
| `PATCH /api/saga/{trackId}?graceful=true` | graceful cancel: finish the current step, then compensate |
| `PATCH /api/saga/{trackId}?graceful=false` | forced shutdown: no compensation, saga row removed |

## Things to try

1. `POST /api/saga/sync/with-result` with no `throwExceptionOnLevel`: an `Audit` comes back, `test_data` and the
   in-memory remote services contain the new entries.
2. The same request with `"throwExceptionOnLevel": 3`: the call fails with the saga exception; `test_data` is
   restored and remote service one has been cleaned up.
3. `POST /api/saga/async` with `"levelToDelay": 2, "syntheticDelayOnEachLevel": "PT30S"`, then
   `PATCH /api/saga/{trackId}?graceful=true` while step 2 is sleeping: step 2 finishes, then
   `deleteOnRemoteServiceOne` and `deleteLocal` run and `GET` reports the cancellation.
4. Repeat with `graceful=false`: the step is interrupted, nothing is compensated, `GET` returns 404-like emptiness
   because the saga row is gone.

`resources/application.yml` contains every saga property with a comment, including per-method settings keyed by
`createOnRemoteServiceOne_v2`.
