## distributed-task-framework
Lightweight task processor based on only database. Support horizontally scaling, affinity protection and map-reduce.

## Introduction
todo

## Install
todo

## Problems
todo

## Solution
todo

## Samples
todo

## Perf test results
| Nodes | max-parallel-tasks-in-cluster-default | batch-size | polling-delay | new-batch-size | max-parallel-tasks-in-node | manage-delay | Total tasks | Total time | RPM                                         | RPS                                       |
|-------|---------------------------------------|------------|---------------|----------------|----------------------------|--------------|-------------|------------|---------------------------------------------|-------------------------------------------|
| 1     | 200                                   | 200        | 10 ms         | 100            | 200                        | 10 ms        | 12_000      | 195 sec    | <span style="color:green"> **3692** </span> | <span style="color:green"> **61** </span> |
| 1     | 200                                   | 200        | 10 ms         | 100            | 200                        | 10 ms        | 120_000     | 1951 sec   | <span style="color:green"> **3692** </span> | <span style="color:green"> **61** </span> |


## How it works?


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

