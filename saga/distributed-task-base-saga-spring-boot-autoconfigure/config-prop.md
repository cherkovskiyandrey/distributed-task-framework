# DSF Spring configuration properties
This is a document about the configuration properties in DSF (saga) autoconfiguration module
## Table of Contents
* [**distributed-task.saga** - `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties`](#distributed-task.saga)
* [**distributed-task.saga.common** - `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$Common`](#distributed-task.saga.common)
* [**distributed-task.saga.saga-method-properties-group** - `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaMethodPropertiesGroup`](#distributed-task.saga.saga-method-properties-group)
* [**distributed-task.saga.saga-properties-group** - `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaPropertiesGroup`](#distributed-task.saga.saga-properties-group)
* [**distributed-task.saga.saga-stat-properties** - `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaStatProperties`](#distributed-task.saga.saga-stat-properties)
* [**distributed-task.saga** - `Unknown`](#distributed-task.saga)
* [**distributed-task.saga.saga-method-properties-group.default-saga-method-properties** - `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaMethodProperties`](#distributed-task.saga.saga-method-properties-group.default-saga-method-properties)
* [**distributed-task.saga.saga-properties-group.default-saga-properties** - `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaProperties`](#distributed-task.saga.saga-properties-group.default-saga-properties)

### distributed-task.saga
**Class:** `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
### distributed-task.saga.common
**Class:** `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$Common`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| cache-expiration| java.time.Duration| Cache expiration for internal purpose. Mostly to cover cases when user invoke method like {@link SagaManager#isCompleted(UUID)}| | | 
| deprecated-saga-scan-fixed-delay| java.time.Duration| Fixed delay to scan deprecation sagas: completed and expired.| | | 
| expired-saga-batch-size-to-remove| java.lang.Integer| The size of batch of expired saga entities in db to handle (remove) in one tick.| | | 
### distributed-task.saga.saga-method-properties-group
**Class:** `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaMethodPropertiesGroup`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| saga-method-properties| java.util.Map&lt;java.lang.String,com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaMethodProperties&gt;| Customisation of configuration for dedicated saga method.| | | 
### distributed-task.saga.saga-properties-group
**Class:** `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaPropertiesGroup`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| saga-properties-group| java.util.Map&lt;java.lang.String,com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaProperties&gt;| Custom properties for particular saga by name. Overrides default one.| | | 
### distributed-task.saga.saga-stat-properties
**Class:** `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaStatProperties`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| calc-fixed-delay| java.time.Duration| Delay between calculation of saga statistics.| | | 
| calc-initial-delay| java.time.Duration| Initial delay between calculation of saga statistics.| | | 
| top-n-sagas| java.lang.Integer| Top N saga names to print in stat.| | | 
### distributed-task.saga
**Class:** `Unknown`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
### distributed-task.saga.saga-method-properties-group.default-saga-method-properties
**Class:** `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaMethodProperties`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| execution-guarantees| com.distributed_task_framework.settings.TaskSettings$ExecutionGuarantees| Execution guarantees.| | | 
| max-parallel-in-cluster| java.lang.Integer| How many parallel saga methods can be in the cluster. &#x27;-1&#x27; means undefined and depends on current cluster configuration like how many pods work simultaneously.| | | 
| no-retry-for| java.util.List&lt;java.lang.Class&lt;? extends java.lang.Throwable&gt;&gt;| List of exceptions saga retry logic not used for. Usually unrecoverable exception where retry doesn&#x27;t matter. 1. Default is not set in config at all. In this case field equals null - means not set. And value will be used from low precedence configs. 2. If is set in code as empty list or not empty - means override all low precedence values.| | | 
| retry| com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Retry| Retry policy for saga-task.| | | 
| timeout| java.time.Duration| Task timeout. If saga method still is in progress after timeout expired, it will be interrupted. {@link InterruptedException} will be risen in {@link Task#execute(ExecutionContext)}| | | 
### distributed-task.saga.saga-properties-group.default-saga-properties
**Class:** `com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties$SagaProperties`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| available-after-completion-timeout| java.time.Duration| Time between completion of saga and removing of its result from db. In other words: time interval during saga result is available.| | | 
| expiration-timeout| java.time.Duration| Default timeout for saga. After timeout expired, the whole saga will be canceled.| | | 
| stop-on-failed-any-revert| java.lang.Boolean| If any revert operation fail, then stop execute reverting chain and complete saga.| | | 


This is a generated file, generated at: **2026-10-28T20:03:38.594916**

