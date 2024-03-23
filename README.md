## distributed-task-framework
Lightweight task processor based on only database. Support horizontally scaling, affinity protection and map-reduce.

## Introduction
todo

---

## Install
todo

---

## Problems
todo

---

## Solution
todo

---

## Samples
todo

---

## Perf test
# Environment 1
- CPU: Apple M1 Pro
- RAM: 16 Gb
- DB: postgres:12 from current docker-compose.yaml

# Results
| Nodes | Logs  | max-parallel-tasks-in-cluster-default | batch-size | polling-delay | new-batch-size | max-parallel-tasks-in-node | manage-delay | plan-factor | Total tasks | Total time | RPM                                         | RPS                                        |
|-------|-------|---------------------------------------|------------|---------------|----------------|----------------------------|--------------|-------------|-------------|------------|---------------------------------------------|--------------------------------------------|
| 1     | off   | -                                     | 100        | 10 ms         | 100            | 100                        | 10 ms        | 5.0         | 12_200      | 104 sec    | <span style="color:green"> **7469** </span> | <span style="color:green"> **124** </span> |
| 1     | off   | 200                                   | 200        | 10 ms         | 100            | 200                        | 10 ms        | 5.0         | 122_000     | 1951 sec   | <span style="color:green"> **3692** </span> | <span style="color:green"> **61** </span>  |


# Environment 2
- CPU: Apple M1 Pro
- RAM: 16 Gb
- DB: postgres:12 from current docker-compose.yaml

# Results
| Nodes | Logs  | max-parallel-tasks-in-cluster-default | batch-size | polling-delay | new-batch-size | max-parallel-tasks-in-node | manage-delay | plan-factor | Total tasks | Total time | RPM                                         | RPS                                        |
|-------|-------|---------------------------------------|------------|---------------|----------------|----------------------------|--------------|-------------|-------------|------------|---------------------------------------------|--------------------------------------------|
| 1     | off   | -                                     | 100        | 10 ms         | 100            | 100                        | 10 ms        | 5.0         | 12_200      | 104 sec    | <span style="color:green"> **7469** </span> | <span style="color:green"> **124** </span> |
| 1     | off   | 200                                   | 200        | 10 ms         | 100            | 200                        | 10 ms        | 5.0         | 122_000     | 1951 sec   | <span style="color:green"> **3692** </span> | <span style="color:green"> **61** </span>  |



# TODO: the pest result on prod DB 12_200 => 154 RPS on one node
```json
{
  "id": 1,
  "name": "1",
  "createdAt": "2024-03-14T22:29:00.640835",
  "completedAt": "2024-03-14T22:30:20.167194",
  "duration": 79.526359,
  "affinityGroup": "afg-1",
  "totalPipelines": 100,
  "totalAffinities": 100,
  "totalTaskOnFirstLevel": 10,
  "totalTaskOnSecondLevel": 10,
  "taskDurationMs": 1,
  "summaryStates": {
    "DONE": 100
  }
}
```

# TODO: the pest result on prod DB 122_000 => ~200 RPS on one node
```json
{
  "id": 3,
  "name": "3",
  "createdAt": "2024-03-14T22:53:33.137113",
  "completedAt": "2024-03-14T23:03:46.831643",
  "duration": 613.69453,
  "affinityGroup": "afg-1",
  "totalPipelines": 1000,
  "totalAffinities": 1000,
  "totalTaskOnFirstLevel": 10,
  "totalTaskOnSecondLevel": 10,
  "taskDurationMs": 1,
  "summaryStates": {
    "DONE": 1000
  }
}
```



---

## How does it work?

---

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

---

## Disclaimer

**(En)**
All information and source code are provided AS-IS, without express or implied warranties.
Use of the source code or parts of it is at your sole discretion and risk.
Cherkovskiy Andrey takes reasonable measures to ensure the relevance of the information posted in this repository,
but it does not assume responsibility for maintaining or updating this repository or its parts outside the framework
established by the company independently and without notifying third parties.

