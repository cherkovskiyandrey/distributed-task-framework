# DTF Spring configuration properties
This is a document about the configuration properties in DTF autoconfiguration module
## Table of Contents
* [**distributed-task.saga** - `com.distributed_task_framework.saga.SagaConfiguration`](#distributed-task.saga)
* [**distributed-task.saga.commons** - `com.distributed_task_framework.saga.SagaConfiguration$Common`](#distributed-task.saga.commons)
* [**distributed-task.saga.context** - `com.distributed_task_framework.saga.SagaConfiguration$Context`](#distributed-task.saga.context)
* [**distributed-task.saga.saga-method-properties-group** - `com.distributed_task_framework.saga.SagaConfiguration$SagaMethodPropertiesGroup`](#distributed-task.saga.saga-method-properties-group)
* [**distributed-task** - `Unknown`](#distributed-task)
* [**distributed-task.saga.saga-method-properties-group.default-saga-method-properties** - `com.distributed_task_framework.saga.SagaConfiguration$SagaMethodProperties`](#distributed-task.saga.saga-method-properties-group.default-saga-method-properties)

### distributed-task.saga
**Class:** `com.distributed_task_framework.saga.SagaConfiguration`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| saga-properties-group| java.util.Map&lt;java.lang.String,com.distributed_task_framework.saga.SagaConfiguration$SagaProperties&gt;| | | | 
### distributed-task.saga.commons
**Class:** `com.distributed_task_framework.saga.SagaConfiguration$Common`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| cache-expiration| java.time.Duration| Cache expiration for internal purpose. Mostly to cover cases when user invoke method like {@link SagaContextService#isCompleted(UUID)}| | | 
### distributed-task.saga.context
**Class:** `com.distributed_task_framework.saga.SagaConfiguration$Context`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| completed-timeout| java.time.Duration| Time between completion of saga and removing of its result from db. In other words: time interval during saga result is available.| | | 
| deprecated-saga-scan-fixed-delay| java.time.Duration| Fixed delay to scan deprecation sagas: completed and expired.| | | 
| deprecated-saga-scan-initial-delay| java.time.Duration| Initial delay to start scan deprecation sagas: completed and expired.| | | 
| expiration-timeout| java.time.Duration| Default timeout for any saga. After timout is expired, the whole saga will be canceled. Can be customized in {@link SagaConfiguration.SagaProperties#expirationTimeout}| | | 
### distributed-task.saga.saga-method-properties-group
**Class:** `com.distributed_task_framework.saga.SagaConfiguration$SagaMethodPropertiesGroup`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| saga-method-properties| java.util.Map&lt;java.lang.String,com.distributed_task_framework.saga.SagaConfiguration$SagaMethodProperties&gt;| | | | 
### distributed-task
**Class:** `Unknown`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
### distributed-task.saga.saga-method-properties-group.default-saga-method-properties
**Class:** `com.distributed_task_framework.saga.SagaConfiguration$SagaMethodProperties`

|Key|Type|Description|Default value|Deprecation|
|---|----|-----------|-------------|-----------|
| execution-guarantees| com.distributed_task_framework.settings.TaskSettings$ExecutionGuarantees| Execution guarantees.| | | 
| max-parallel-in-cluster| java.lang.Integer| How many parallel saga methods can be in the cluster. &#x27;-1&#x27; means undefined and depends on current cluster configuration like how many pods work simultaneously.| | | 
| retry| com.distributed_task_framework.autoconfigure.DistributedTaskProperties$Retry| Retry policy for saga-task.| | | 
| timeout| java.time.Duration| Task timeout. If saga method still is in progress after timeout expired, it will be interrupted. {@link InterruptedException} will be risen in {@link Task#execute(ExecutionContext)}| | | 


This is a generated file, generated at: **2024-11-02T22:43:44.768015**

