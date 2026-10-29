# Saga test utilities

Spring Boot test support for applications built on the
[Distributed Saga Framework](../../saga/README.md). It adds a `SagaTestUtil` bean on top of the
[DTF test utilities](../dtf/README.md), so a test can reset sagas and their underlying tasks in one call.

<!-- TOC -->
* [Saga test utilities](#saga-test-utilities)
  * [Modules](#modules)
  * [Install](#install)
  * [Activation](#activation)
  * [Quick start](#quick-start)
  * [SagaTestUtil](#sagatestutil)
  * [Recipes](#recipes)
    * [Testing the happy path](#testing-the-happy-path)
    * [Testing compensation](#testing-compensation)
    * [Testing cancellation](#testing-cancellation)
    * [Speeding up retries](#speeding-up-retries)
  * [License](#license)
<!-- TOC -->

## Modules

| Module | Artifact | Contents |
|---|---|---|
| `distributed-task-base-saga-test-spring-boot-autoconfigure` | `io.github.cherkovskiyandrey:distributed-task-base-saga-test-spring-boot-autoconfigure` | `TestSagaAutoconfiguration`, `SagaTestUtil`. Depends on the DTF test autoconfigure module. |
| `distributed-task-base-saga-test-spring-boot-starter` | `io.github.cherkovskiyandrey:distributed-task-base-saga-test-spring-boot-starter` | Starter: the module above + `spring-boot-starter`. This is what you add to a project. |

## Install

```groovy
testImplementation 'io.github.cherkovskiyandrey:distributed-task-base-saga-test-spring-boot-starter:2.0.0-rc.2'
```

```xml
<dependency>
    <groupId>io.github.cherkovskiyandrey</groupId>
    <artifactId>distributed-task-base-saga-test-spring-boot-starter</artifactId>
    <version>2.0.0-rc.2</version>
    <scope>test</scope>
</dependency>
```

The DTF test starter comes transitively; you do not need to add it separately.

## Activation

`TestSagaAutoconfiguration` is active when all of the following hold:

1. the Spring profile `test` is active;
2. `distributed-task.enabled=true`;
3. `distributed-task.saga.enabled=true`;
4. the saga starter is on the classpath.

All DTF test optimisations (fast planner and worker settings, mocked cluster provider, cron tasks on demand) come
from the DTF test autoconfiguration, which is activated by the same profile. See
[../dtf/README.md](../dtf/README.md#what-the-test-autoconfiguration-changes).

## Quick start

```java
@ActiveProfiles("test")
@SpringBootTest(properties = {
    "distributed-task.enabled=true",
    "distributed-task.saga.enabled=true",
    "distributed-task.common.app-name=test"
})
class OrderSagaTest {

    @Autowired
    OrderSagaService orderSagaService;
    @Autowired
    SagaTestUtil sagaTestUtil;
    @Autowired
    OrderRepository orderRepository;

    @BeforeEach
    void reset() throws Exception {
        sagaTestUtil.reinitAndWait();
    }

    @Test
    void shouldPlaceOrder() throws Exception {
        OrderRequest request = new OrderRequest(UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("10.00"));

        OrderConfirmation confirmation = orderSagaService.placeOrder(request)
            .get(Duration.ofSeconds(30))
            .orElseThrow();

        assertThat(confirmation.orderId()).isEqualTo(request.orderId());
        assertThat(orderRepository.findById(request.orderId())).isPresent();
    }
}
```

The test Liquibase master must include both changelogs:

```yaml
databaseChangeLog:
  - include:
      file: db/changelog/distributed-task-framework/db.changelog-aggregator.yaml
  - include:
      file: db/changelog/distributed-task-base-saga/db.changelog-aggregator.yaml
```

## SagaTestUtil

```java
public interface SagaTestUtil {
    void reinitAndWait() throws InterruptedException;
    void reinitAndWait(int attemptsToCancel, Duration duration) throws InterruptedException;
}
```

`reinitAndWait` does three things:

1. calls `DistributedTaskTestUtil.reinitAndWait(...)` with the internal saga manager task excluded, so every saga
   step and revert task is cancelled and awaited while the housekeeping task keeps running;
2. deletes all rows from `_____dtf_saga`;
3. deletes all rows from `_____dtf_saga_dls`.

After it returns, `getFlow(trackId)` for any previously started saga throws `SagaNotFoundException`. Defaults are
10 cancellation attempts and a 1 minute wait; `FailedCancellationException` is thrown when tasks cannot be
stopped in time.

`SagaTestUtil` is a separate bean; `DistributedTaskTestUtil` is still available if you need the task-level reset
on its own.

## Recipes

### Testing the happy path

Use `get(Duration)` on the returned flow; with test settings a short saga completes in well under a second.
Assert both the returned result and the side effects of every step.

### Testing compensation

Make a step fail deterministically (a test double for the external service, or a flag in the input), then assert
that `get()` throws and that the compensations ran:

```java
@Test
void shouldCompensateWhenPaymentFails() {
    paymentGateway.failNextChargeWith(new InsufficientFundsException());
    OrderRequest request = newRequest();

    SagaFlow<OrderConfirmation> flow = orderSagaService.placeOrder(request);

    assertThatThrownBy(() -> flow.get(Duration.ofSeconds(30)))
        .isInstanceOf(SagaExecutionException.class)
        .satisfies(e -> assertThat(((SagaExecutionException) e)
            .isCauseRelatedTo(InsufficientFundsException.class)).isTrue());
    assertThat(orderRepository.findById(request.orderId())).isEmpty(); // deleteOrder ran
    assertThat(paymentGateway.refunds()).isEmpty();                     // refundPayment ran with receipt == null
}
```

`get()` returns only after the whole revert chain has finished, so no extra waiting is needed before asserting
on compensation side effects.

### Testing cancellation

Block a step on a latch, start the saga, call `cancel(true)` on the flow, release the latch and assert that
`get()` throws `SagaCancellationException` and that the compensations of the completed steps ran. For `cancel(false)`
assert `SagaNotFoundException` and that nothing was compensated.

### Speeding up retries

Saga steps retry with the DTF default policy (10 s fixed delay). In tests that exercise retries, shorten it:

```yaml
distributed-task:
  saga:
    saga-method-properties-group:
      default-saga-method-properties:
        retry:
          retry-mode: FIXED
          fixed:
            delay: PT0.1S
            max-number: 3
```

and keep `expiration-timeout` well above the longest test, otherwise the saga is shut down without compensation
and `get()` throws `SagaNotFoundException`.

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
