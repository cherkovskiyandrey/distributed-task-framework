# DTF Test Utilities

Test utilities for Distributed Task Framework (DTF) that provide comprehensive support for testing distributed tasks.

<!-- TOC -->
* [DTF Test Utilities](#dtf-test-utilities)
  * [Introduction](#introduction)
    * [Module Structure](#module-structure)
    * [Features](#features)
  * [Install](#install)
  * [Quick Start](#quick-start)
    * [Basic Task Testing](#basic-task-testing)
  * [Modules](#modules)
    * [distributed-task-test](#distributed-task-test)
      * [DistributedTaskTestUtil](#distributedtasktestutil)
      * [Test Configuration](#test-configuration)
      * [Test Autoconfiguration](#test-autoconfiguration)
<!-- TOC -->

## Introduction

DTF Test Utilities provide a testing spring framework for distributed tasks. The utilities offer test isolation, mocked infrastructure components, and convenient helpers for writing integration and unit tests.

### Module Structure

The test utilities consist of two main modules:

1. **distributed-task-test** - Core testing utilities for DTF
2. **distributed-task-test-spring-boot-autoconfigure** - Configuration for spring boot applications
3. **distributed-task-test-spring-boot-starter** - Spring Boot Starter module to use in test scope

### Features

- Test isolation and cleanup utilities
- Mocked cluster provider for faster tests
- Cron task control in tests

## Install

Add the test utilities to your test dependencies:

```groovy
testImplementation 'io.github.cherkovskiyandrey:distributed-task-test-spring-boot-starter'
```

## Quick Start

### Basic Task Testing

```java
@SpringBootTest
@ActiveProfiles("test")
public class MyTaskTest {

    @Autowired
    private DistributedTaskService distributedTaskService;

    @Autowired
    private DistributedTaskTestUtil testUtil;

    @BeforeEach
    public void init() {
        // Clean up any existing tasks and wait for end of operation. 
        testUtil.reinitAndWait();
    }
    
    @Test
    public void testMyTask() throws Exception {
        // Schedule your task
        TaskId taskId = distributedTaskService.schedule(
            MyTask.MY_TASK_DEF,
            ExecutionContext.simple(new MyMessage("test"))
        );

        // Wait for completion and verify
        // ... your assertions here
    }
}
```

## Modules

### distributed-task-test

The core testing module providing essential utilities for DTF testing.

#### DistributedTaskTestUtil

Main utility class for test management:

```java
public interface DistributedTaskTestUtil {
    // Cancel all tasks and wait for completion
    void reinitAndWait();

    // With custom parameters
    void reinitAndWait(int attemptsToCancel, Duration duration);

    // Excluding specific tasks
    void reinitAndWait(int attemptsToCancel, Duration duration, List<String> excludeList);
}
```

**Key methods:**
- `reinitAndWait()` - Cancels all active tasks and waits for completion
- `reinitAndWait(attempts, duration)` - Custom retry attempts and timeout
- `reinitAndWait(attempts, duration, excludeList)` - Exclude specific tasks from cancellation

#### Test Configuration

The module provides optimized test configuration via `application-dtf-test-utils.yml`:

```yaml
distributed-task:
  enabled: true
  common:
    planner:
      # Optimized for faster execution in tests
      polling-delay:
        0: 10
    worker-manager:
      # Faster task management
      manage-delay:
        0: 10
...
```

#### Test Autoconfiguration

`TestDistributedTaskAutoconfiguration` provides:
- Mocked `ClusterProvider` to eliminate node registration delays
- `DistributedTaskTestUtil` bean registration
- Optimized default configuration to speedup tests 
- Disable all cron tasks (can be tuned via distributed-task.test.cron.enabled property)

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