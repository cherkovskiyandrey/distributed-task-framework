# Distributed Task Framework

A lightweight task processor based on database only. Supports horizontal scaling, affinity protection and map-reduce.

<!-- TOC -->
* [Distributed Task Framework](#distributed-task-framework)
  * [Overview](#overview)
  * [Project Structure](#project-structure)
    * [Core - Distributed Task Framework (DTF)](#core---distributed-task-framework-dtf)
      * [Core Components](#core-components)
    * [Saga - Distributed Saga Framework (DSF)](#saga---distributed-saga-framework)
      * [Saga Components](#saga-components)
    * [Test Utils](#test-utils)
      * [Documentation](#documentation)
  * [Key Features](#key-features)
    * [Distributed Task Framework (DTF)](#distributed-task-framework-dtf)
    * [Saga Framework (DSF)](#saga-framework)
  * [Documentation](#documentation-1)
  * [License](#license)
  * [Disclaimer](#disclaimer)
<!-- TOC -->

## Overview

This project provides a comprehensive framework for distributed task processing and saga pattern implementation for managing long-running transactions across microservices.

## Project Structure

### Core - Distributed Task Framework (DTF)

The core module contains the main distributed task processing library that enables reliable task execution across multiple service instances.

#### Core Components:

- **core/distributed-task-framework** - the main library implementation containing business logic with minimal external dependencies. This is the heart of the framework that handles task scheduling, execution, and persistence.

- **core/distributed-task-spring-boot-autoconfigure** - The Spring Boot auto-configuration module that provides automatic configuration and bean setup for easy integration with Spring Boot applications.

- **core/distributed-task-spring-boot-starter** - The Spring Boot starter module that serves as the entry point for adding DTF capabilities to any Spring project with minimal configuration.

**[README.md](core/README.md)** - Complete DTF documentation

### Saga - Distributed Saga Framework

The saga module implements the saga pattern for managing distributed transactions with compensating transactions, built on top of DTF.

#### Saga Components:

- **saga/distributed-task-base-saga-library** - the core saga implementation containing the main logic for saga orchestration, compensation management, and state tracking.

- **saga/distributed-task-base-saga-spring-boot-autoconfigure** - Spring Boot auto-configuration for the saga framework, providing automatic setup of saga-related beans and configuration.

- **saga/distributed-task-base-saga-spring-boot-starter** - Spring Boot starter for easy integration of saga capabilities into Spring applications.

**[README.md](saga/README.md)** - Complete Saga documentation

### Test Utils

Utilities and helpers for convenient integration testing with DTF and Saga frameworks.

#### Documentation:
- **[test-utils/dtf/README.md](test-utils/dtf/README.md)** - Testing utilities documentation for DTF
- **[test-utils/dsf/README.md](test-utils/dsf/README.md)** - Testing utilities documentation for DSF

## Key Features

### Distributed Task Framework (DTF)
- At-least-once and exactly-once execution guarantees
- Configurable retry policies (fixed, backoff, disabled)
- Rate limiting and timeout management
- Affinity protection for resource coordination
- Map-Reduce support for parallel computations
- Dead Letter Queue for failed tasks
- Workflow support for task grouping
- Cron-based scheduled tasks

### Saga Framework (DSF)
- Compensating transactions for reliable rollbacks
- Asynchronous and synchronous execution modes
- Versioning support for rolling updates
- Affinity groups for related transactions
- Comprehensive error handling and recovery
- Integration with DTF's persistence and retry mechanisms

## Documentation

For detailed information, please refer to:
- [Complete DTF Documentation](core/README.md)
- [Complete Saga Documentation](saga/README.md)
- [DTF Testing Utilities](test-utils/dtf/README.md)
- [Saga Testing Utilities](test-utils/dsf/README.md)

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

**(En)**
All information and source code are provided AS-IS, without express or implied warranties.
Use of the source code or parts of it is at your sole discretion and risk.
Cherkovskiy Andrey takes reasonable measures to ensure the relevance of the information posted in this repository,
but it does not assume responsibility for maintaining or updating this repository or its parts outside the framework
established by the company independently and without notifying third parties.