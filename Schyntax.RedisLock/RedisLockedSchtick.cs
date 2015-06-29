using System;
using System.Collections.Generic;
using System.Diagnostics;
using StackExchange.Redis;

namespace Schyntax.RedisLock
{
    public class RedisLockedSchtick
    {
        private readonly Schtick _schtick = new Schtick();
        private readonly Func<IDatabase> _getRedisDb;
        private readonly string _machineName;
        private readonly string _keyPrefix;
        private readonly RedisKey _lastKey;

        public RedisLockedSchtick(Func<IDatabase> getRedisDb, string machineName = null, string keyPrefix = "schyntax")
        {
            _getRedisDb = getRedisDb;
            _keyPrefix = keyPrefix;
            _lastKey = keyPrefix + "_last";
            _machineName = machineName ?? Environment.MachineName;
        }

        public ScheduledTask AddTask(
            string name,
            string schedule,
            ScheduledTaskCallback callback,
            bool autoRun = true,
            TimeSpan window = default(TimeSpan),
            bool skipIfSlowCallback = true)
        {
            DateTime lastRun = default(DateTime);
            if (window > TimeSpan.Zero)
            {
                // check for last run time
                var db = _getRedisDb();
                var value = db.HashGet(_lastKey, name);
                if (value.HasValue)
                {
                    string str = value;
                    var i = str.IndexOf(';');
                    if (i != -1)
                    {
                        DateTime.TryParse(str.Substring(0, i), out lastRun);
                    }
                }
            }

            return _schtick.AddTask(
                schedule,
                CreateWrappedCallback(name, callback, window),
                autoRun,
                name,
                lastRun,
                window,
                skipIfSlowCallback
            );
        }

        public bool RemoveTask(ScheduledTask task)
        {
            return _schtick.RemoveTask(task);
        }

        private const string REDIS_LOCK_SCRIPT_BODY = @"
if redis.call('set', @lockKey, @host, 'nx', 'px', @px)
then
    redis.call('hset', @lastKey, @name, @lastLockValue)
    return 1
else
    return 0
end
";
        private static readonly LuaScript s_redisLockScript = LuaScript.Prepare(REDIS_LOCK_SCRIPT_BODY);

        private ScheduledTaskCallback CreateWrappedCallback(string name, ScheduledTaskCallback originalCallback, TimeSpan window)
        {
            // set the redis lock for one hour longer than the window
            var px = (int)((window > TimeSpan.Zero ? window : TimeSpan.Zero) + TimeSpan.FromHours(1)).TotalMilliseconds;
            var host = _machineName;
            var lastKey = _lastKey;

            return (task, timeIntendedToRun) =>
            {
                // see if we can get the lock on this task
                var iso = timeIntendedToRun.ToString("o");
                RedisKey lockKey = _keyPrefix + ";" + name + ";" + iso;
                var lastLockValue = iso + ";" + DateTime.UtcNow.ToString("o") + ";" + host;

                try
                { 
                    var db = _getRedisDb();
                    var lockAcquired = (int)db.ScriptEvaluate(s_redisLockScript, new { lockKey, host, px, lastKey, name, lastLockValue });

                    if (lockAcquired == 1)
                    {
                        // we got the lock, now run the task
                        originalCallback(task, timeIntendedToRun);
                    }
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(ex);
                }
            };
        }
    }
}
