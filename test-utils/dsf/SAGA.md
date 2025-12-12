# Saga Test Utilities

Test utilities for Distributed Task Framework Saga module that provide comprehensive support for testing distributed sagas.

<!-- TOC -->
* [Saga Test Utilities](#saga-test-utilities)
  * [Introduction](#introduction)
    * [Module Structure](#module-structure)
    * [Features](#features)
  * [Install](#install)
  * [Quick Start](#quick-start)
    * [Basic Saga Testing](#basic-saga-testing)
  * [Modules](#modules)
    * [distributed-task-base-saga-test-spring-boot-autoconfigure](#distributed-task-base-saga-test-spring-boot-autoconfigure)
      * [SagaTestUtil](#sagatestutil)
      * [Test Configuration](#test-configuration)
      * [Test Autoconfiguration](#test-autoconfiguration)
  * [Testing Patterns](#testing-patterns)
    * [Saga Isolation](#saga-isolation)
  * [Best Practices](#best-practices)
<!-- TOC -->

## Introduction

Saga Test Utilities provide a spring boot testing framework for distributed sagas. The utilities extend DTF test capabilities with saga-specific cleanup, transaction management, and compensation testing.

### Module Structure

The saga test utilities consist of two main modules:

1. **distributed-task-base-saga-test-spring-boot-autoconfigure** - Configuration for spring boot applications
2. **distributed-task-base-saga-test-spring-boot-starter** - Spring Boot Starter module to use in test scope

### Features

- Saga isolation and cleanup utilities
- Distributed transaction testing support

## Install

Add the saga test utilities to your test dependencies:

```groovy
testImplementation 'io.github.cherkovskiyandrey:distributed-task-base-saga-test-spring-boot-starter'
```

## Quick Start

### Basic Saga Testing

```java
@SpringBootTest
@ActiveProfiles("test")
public class MySagaTest {

    @Autowired
    private DistributionSagaService distributionSagaService;

    @Autowired
    private SagaTestUtil sagaTestUtil;

    @BeforeEach
    public void init() {
        // Clean up any existing saga data
        sagaTestUtil.reinitAndWait();
    }
    
    @Test
    public void testMySaga() throws Exception {
        // Execute saga
        var actualPayment = sagaService
            .createWithAffinity("order-processing", "order", request.getOrderId())
            .registerToRun(
                orderService::createOrder,
                orderService::revertCreateOrder,
                request
            )
            .thenRun(
                orderService::processPayment,
                orderService::refundPayment
            )
            .start()
            .get();
        
        assertThat(actualPayment).isEqualTo(expectedPayment);
    }
}
```

## Modules

### distributed-task-base-saga-test-spring-boot-autoconfigure

The core saga testing module providing essential utilities for saga testing.

#### SagaTestUtil

Main utility class for saga test management, extends `DistributedTaskTestUtil`:

```java
public interface SagaTestUtil {
    
    void reinitAndWait() throws InterruptedException;
    
    void reinitAndWait(int attemptsToCancel, Duration duration) throws InterruptedException;
}
```

**Key features:**
- Automatically cleans up both tasks and saga data
- Ensures complete test isolation
- Handles saga state cleanup during `reinitAndWait()`

#### Test Autoconfiguration

`TestSagaAutoconfiguration` provides:
- `SagaTestUtil` bean registration
- Integration with DTF test configuration
- Saga service beans for testing

## Testing Patterns

### Saga Isolation

Always clean up before and after saga tests:

```java
@BeforeEach
void setUp() {
    sagaTestUtil.reinitAndWait();
}

@AfterEach
void tearDown() {
    sagaTestUtil.reinitAndWait();
}
```


## Best Practices

1. **Always use SagaTestUtil** - Ensures both tasks and saga data are cleaned up
2. **Test both success and failure paths** - Verify compensations work correctly
3. **Use appropriate timeouts** - Sagas may take longer than regular tasks
4. **Mock external dependencies** - Use test doubles for external services
5. **Test saga versioning** - Verify saga evolution works correctly
6. **Clean up resources** - Ensure compensations properly clean up resources
7. **Test concurrent sagas** - Verify saga isolation under concurrent execution
8. **Validate business invariants** - Check that business rules are maintained

## License

Copyright 2025 Cherkovskiy Andrey

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

**(En)**
All information and source code are provided AS-IS, without express or implied warranties.
Use of the source code or parts of it is at your sole discretion and risk.
Cherkovskiy Andrey takes reasonable measures to ensure the relevance of the information posted in this repository,
but it does not assume responsibility for maintaining or updating this repository or its parts outside the framework
established by the company independently and without notifying third parties.