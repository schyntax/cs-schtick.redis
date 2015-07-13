# Schyntax.RedisLock

Uses Redis to provide a distributed lock so that for each iteration of a task, the callback will only be called on one server.

# Basic Usage

### Task Locking

To lock a task so that it only runs on one server, use the `Wrap()` on the intended callback for the task:

```csharp
var schtick = new Schtick();
var redis = ConnectionMultiplexer.Connect("localhost:6379");
var wrapper = new RedisSchtickWrapper(() => redis.GetDatabase());

schtick.AddTask("unique-task-name", "min(*)", wrapper.Wrap((task, timeIntendedToRun) =>
{
	// your callback code here
}));
```

### Get Last Run Event

The `Schtick.AddTask()` method has an optional `lastKnownRun` parameter which is used in conjunction with the `window` parameter. If you want to specify a window, you can get the last run time from redis first using the `GetLastRunTime()` or `GetLastRunTimeAsync()` methods.

```csharp
var schtick = new Schtick();
var redis = ConnectionMultiplexer.Connect("localhost:6379");
var wrapper = new RedisSchtickWrapper(() => redis.GetDatabase());

var lastKnownRun = wrapper.GetLastRunTime("unique-task-name");
schtick.AddTask("unique-task-name", "hours(*)", wrapper.Wrap((task, timeIntendedToRun) =>
{
	// your callback code here
	
}), lastKnownRun: lastKnownRun, window: TimeSpan.FromMinutes(10));
```
