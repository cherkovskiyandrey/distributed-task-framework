# DTF Spring configuration properties
This is a document about the configuration properties in DTF autoconfiguration module
## Table of Contents
* [**distributed-task.common.delivery-manager.remote-apps** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$RemoteApps`](#distributed-task.common.delivery-manager.remote-apps)
* [**distributed-task.common.delivery-manager.retry** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Retry`](#distributed-task.common.delivery-manager.retry)
* [**distributed-task.task-properties-group.default-properties.retry** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Retry`](#distributed-task.task-properties-group.default-properties.retry)
* [**distributed-task.common.delivery-manager.retry.backoff** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Backoff`](#distributed-task.common.delivery-manager.retry.backoff)
* [**distributed-task.common.delivery-manager.retry.fixed** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Fixed`](#distributed-task.common.delivery-manager.retry.fixed)
* [**distributed-task.task-properties-group.default-properties.retry.backoff** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Backoff`](#distributed-task.task-properties-group.default-properties.retry.backoff)
* [**distributed-task.task-properties-group.default-properties.retry.fixed** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Fixed`](#distributed-task.task-properties-group.default-properties.retry.fixed)
* [**distributed-task** - `Unknown`](#distributed-task)
* [**distributed-task** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties`](#distributed-task)
* [**distributed-task.common** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Common`](#distributed-task.common)
* [**distributed-task.task-properties-group** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$TaskPropertiesGroup`](#distributed-task.task-properties-group)
* [**distributed-task.task-properties-group.default-properties** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$TaskProperties`](#distributed-task.task-properties-group.default-properties)
* [**distributed-task.common.delivery-manager** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$DeliveryManager`](#distributed-task.common.delivery-manager)
* [**distributed-task.common.planner** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Planner`](#distributed-task.common.planner)
* [**distributed-task.common.registry** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Registry`](#distributed-task.common.registry)
* [**distributed-task.common.statistics** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Statistics`](#distributed-task.common.statistics)
* [**distributed-task.common.worker-manager** - `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$WorkerManager`](#distributed-task.common.worker-manager)

### distributed-task.common.delivery-manager.remote-apps
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$RemoteApps`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| app-to-url| java.util.Map&lt;java.lang.String,java.net.URL&gt;| Mapping of name remote application to its URL. Used in order to execute remote (outside of current cluster) tasks.| | | 
### distributed-task.common.delivery-manager.retry
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Retry`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| retry-mode| java.lang.String| Sets default retry mode.| BACKOFF| | 
### distributed-task.task-properties-group.default-properties.retry
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Retry`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| retry-mode| java.lang.String| Sets default retry mode.| BACKOFF| | 
### distributed-task.common.delivery-manager.retry.backoff
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Backoff`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| delay-period| java.time.Duration| The time interval that is the ratio of the exponential backoff formula (geometric progression).| PT10S| | 
| initial-delay| java.time.Duration| Initial delay of the first retry.| PT10S| | 
| max-delay| java.time.Duration| Maximum amount of time waiting before retrying.| PT1H| | 
| max-retries| java.lang.Integer| Maximum number of times a tuple is retried before being acked and scheduled for commit.| 32| | 
### distributed-task.common.delivery-manager.retry.fixed
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Fixed`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| delay| java.time.Duration| Delay between retires.| PT10S| | 
| max-interval| java.time.Duration| Max interval for retires. Give up after whether max attempts is reached or interval is passed.| | | 
| max-number| java.lang.Integer| Max attempts.| 6| | 
### distributed-task.task-properties-group.default-properties.retry.backoff
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Backoff`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| delay-period| java.time.Duration| The time interval that is the ratio of the exponential backoff formula (geometric progression).| PT10S| | 
| initial-delay| java.time.Duration| Initial delay of the first retry.| PT10S| | 
| max-delay| java.time.Duration| Maximum amount of time waiting before retrying.| PT1H| | 
| max-retries| java.lang.Integer| Maximum number of times a tuple is retried before being acked and scheduled for commit.| 32| | 
### distributed-task.task-properties-group.default-properties.retry.fixed
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Fixed`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| delay| java.time.Duration| Delay between retires.| PT10S| | 
| max-interval| java.time.Duration| Max interval for retires. Give up after whether max attempts is reached or interval is passed.| | | 
| max-number| java.lang.Integer| Max attempts.| 6| | 
### distributed-task
**Class:** `Unknown`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| distributed-task.enabled| java.lang.Boolean| If true, DTF will not be started.| false| | 
### distributed-task
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
### distributed-task.common
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Common`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| app-name| java.lang.String| Current application name.| | | 
### distributed-task.task-properties-group
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$TaskPropertiesGroup`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| task-properties| java.util.Map&lt;java.lang.String,com.distributed_task_framework.autoconfigure.DistributedTaskProperties$TaskProperties&gt;| | | | 
### distributed-task.task-properties-group.default-properties
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$TaskProperties`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| cron| java.lang.String| Cron string.| | | 
| dlt-enabled| java.lang.Boolean| Has this task to be moved to dlt when eventually failed.| true| | 
| execution-guarantees| java.lang.String| Execution guarantees.| AT_LEAST_ONCE| | 
| max-parallel-in-cluster| java.lang.Integer| How many parallel tasks can be in the cluster. &#x27;-1&#x27; means undefined and depends on current cluster configuration.| -1| | 
| max-parallel-in-node| java.lang.Integer| How many parallel tasks can be on the one node (one worker). &#x27;-1&#x27; means undefined.| -1| | 
| timeout| java.time.Duration| Task timeout. If task still is in progress after timeout expired, it will be interrupted. {@link InterruptedException} will be risen in {@link Task#execute(ExecutionContext)}| 0| | 
### distributed-task.common.delivery-manager
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$DeliveryManager`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| batch-size| java.lang.Integer| How many commands can be sent in one request.| 100| | 
| connection-timeout| java.time.Duration| Timeout to connect to remote app.| PT5S| | 
| manage-delay| java.util.Map&lt;java.lang.Integer,java.lang.Integer&gt;| Function describe delay between polling remote commands depend on input command number. Key is a number of command in last polling. Value is a delay in ms before next polling.| | | 
| read-timeout| java.time.Duration| The connection is closed when there is no inbound traffic during this time from remote app.| PT5S| | 
| response-timeout| java.time.Duration| Specifies the maximum duration allowed between each network-level read operation while reading a given response from remote app.| PT10S| | 
| watchdog-fixed-delay-ms| java.lang.Integer| Delay between watching by current active planner in ms.| 5000| | 
| watchdog-initial-delay-ms| java.lang.Integer| Initial delay before start to watch by current active planner in ms.| 5000| | 
| write-timeout| java.time.Duration| The connection is closed when a write operation cannot finish in this time.| PT30S| | 
### distributed-task.common.planner
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Planner`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| affinity-group-scanner-time-overlap| java.time.Duration| Time overlap is used in order to scan affinity groups from NEW virtual queue.| PT1M| | 
| batch-size| java.lang.Integer| Max number tasks to plan.| 1000| | 
| deleted-batch-size| java.lang.Integer| Batch size to handle tasks from DELETED virtual queue.| 300| | 
| max-parallel-tasks-in-cluster-default| java.lang.Integer| How many parallel tasks can be in the cluster for unknown task on planner. -1 means unlimited.| -1| | 
| new-batch-size| java.lang.Integer| Batch size to move tasks from NEW virtual queue.| 300| | 
| node-cpu-loading-limit| java.lang.Double| The limit of cpu loading for nodes. If node reach this limit, planner will not consider this node to plan. Range from 0.0-1.0.| 0.95| | 
| partition-tracking-time-window| java.time.Duration| Time window to track pairs of affinityGroup and taskName in ACTIVE virtual queue.| PT1M| | 
| plan-factor| java.lang.Float| Used to plan bigger than capacity in this factor times. In order to reduce potential delay between planner loop steps. Take into account that work only for unlimited tasks (maxParallelTasksInClusterDefault &#x3D; UNLIMITED_PARALLEL_TASKS and TaskSettings.maxParallelInCluster &#x3D; UNLIMITED_PARALLEL_TASKS)| 2.0| | 
| polling-delay| java.util.Map&lt;java.lang.Integer,java.lang.Integer&gt;| Function describe delay between polling of db depends on last number of ready to plan tasks. Key is a number of tasks in last polling. Value is a delay in ms before next polling.| | | 
| watchdog-fixed-delay-ms| java.lang.Integer| Delay between watching by current active planner in ms.| 5000| | 
| watchdog-initial-delay-ms| java.lang.Integer| Initial delay before start to watch by current active planner in ms.| 5000| | 
### distributed-task.common.registry
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Registry`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| cache-expiration-ms| java.lang.Integer| Cache expiration in sec to for registered cluster information.| 1000| | 
| cpu-calculating-time-window| java.time.Duration| Duration of time cpu loading of current node is calculated in.| PT5M| | 
| max-inactivity-interval-ms| java.lang.Integer| Max interval to unregister node when node doesn&#x27;t update status.| 20000| | 
| update-fixed-delay-ms| java.lang.Integer| Delay between updates of node state in ms.| 4000| | 
| update-initial-delay-ms| java.lang.Integer| Initial delay before start to update node state in ms.| 0| | 
### distributed-task.common.statistics
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Statistics`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| calc-fixed-delay-ms| java.lang.Integer| Delay between calculation of task statistics in ms.| 10000| | 
| calc-initial-delay-ms| java.lang.Integer| Initial delay before start to calculate task statistic in ms.| 10000| | 
### distributed-task.common.worker-manager
**Class:** `com.distributed_task_framework.autoconfigure.DistributedTaskProperties$WorkerManager`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| manage-delay| java.util.Map&lt;java.lang.Integer,java.lang.Integer&gt;| Function describe delay between manage of tasks depend on input tasks number. Key is a number of tasks in last polling. Value is a delay in ms before next polling.| | | 
| max-parallel-tasks-in-node| java.lang.Integer| How many parallel tasks can be run on one node.| 100| | 


This is a generated file, generated at: **2025-02-28T21:39:05.061697**

