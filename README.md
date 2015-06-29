# Schyntax.RedisLock

Uses Redis to provide a distributed lock so that for each iteration of a task, the callback will only be called on one server.

# Basic Usage

```csharp
var redis = ConnectionMultiplexer.Connect("localhost:6379");
var schtick = new RedisLockedSchtick(() => redis.GetDatabase());

schtick.AddTask("unique-task-name", "min(*)", (task, timeIntendedToRun) =>
{
	// your callback code here
});
```
