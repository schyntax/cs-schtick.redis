using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Schyntax
{
    public class RedisSchtickWrapper
    {
        public struct RedisSchtickEventInfo
        {
            public DateTime ScheduledTime { get; }
            public DateTime ActualTime { get; }
            public string Host { get; }

            internal RedisSchtickEventInfo(DateTime scheduled, DateTime actual, string host)
            {
                ScheduledTime = scheduled;
                ActualTime = actual;
                Host = host;
            }
        }

        public Schtick Schtick { get; } = new Schtick();

        private readonly Func<IDatabase> _getRedisDb;
        private readonly string _machineName;
        private readonly string _keyPrefix;
        private readonly RedisKey _lastKey;

        public RedisSchtickWrapper(Func<IDatabase> getRedisDb, string machineName = null, string keyPrefix = "schyntax")
        {
            _getRedisDb = getRedisDb;
            _keyPrefix = keyPrefix;
            _lastKey = keyPrefix + "_last";
            _machineName = machineName ?? Environment.MachineName;
        }

        public RedisSchtickEventInfo GetLastRunInfo(string taskName)
        {
            var db = _getRedisDb();
            var value = db.HashGet(_lastKey, taskName);
            return LastRunValueToInfo(value);
        }

        public async Task<RedisSchtickEventInfo> GetLastRunInfoAsync(string taskName)
        {
            var db = _getRedisDb();
            var value = await db.HashGetAsync(_lastKey, taskName);
            return LastRunValueToInfo(value);
        }

        private static RedisSchtickEventInfo LastRunValueToInfo(RedisValue value)
        {
            var info = default(RedisSchtickEventInfo);
            if (value.HasValue)
            {
                string str = value;
                var parts = str.Split(';');

                DateTime scheduled;
                DateTime actual;
                if (parts.Length >= 3 && DateTime.TryParse(parts[0], out scheduled) && DateTime.TryParse(parts[1], out actual))
                {
                    return new RedisSchtickEventInfo(scheduled, actual, parts[2]);
                }
            }

            return info;
        }
        
        public ScheduledTaskAsyncCallback Wrap(ScheduledTaskCallback callback, Func<ScheduledTask, DateTime, bool> shouldTryToRun = null)
        {
            if (callback == null)
                throw new ArgumentNullException(nameof(callback));

            return GetWrappedCallback(callback, null, shouldTryToRun);
        }
        
        public ScheduledTaskAsyncCallback WrapAsync(ScheduledTaskAsyncCallback asyncCallback, Func<ScheduledTask, DateTime, bool> shouldTryToRun = null)
        {
            if (asyncCallback == null)
                throw new ArgumentNullException(nameof(asyncCallback));

            return GetWrappedCallback(null, asyncCallback, shouldTryToRun);
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

        private ScheduledTaskAsyncCallback GetWrappedCallback(
            ScheduledTaskCallback originalCallback, 
            ScheduledTaskAsyncCallback originalAsyncCallback, 
            Func<ScheduledTask, DateTime, bool> shouldTryToRun)
        {
            var host = _machineName;
            var lastKey = _lastKey;

            return async (task, timeIntendedToRun) =>
            {
                if (shouldTryToRun?.Invoke(task, timeIntendedToRun) == false)
                    return;

                var iso = timeIntendedToRun.ToString("o");
                RedisKey lockKey = _keyPrefix + ";" + task.Name + ";" + iso;
                var lastLockValue = iso + ";" + DateTime.UtcNow.ToString("o") + ";" + host;

                // set the redis lock for one hour longer than the window
                var window = task.Window;
                var expiry = (window > TimeSpan.Zero ? window : TimeSpan.Zero) + TimeSpan.FromHours(1);
                var px = expiry.TotalMilliseconds;

                // see if we can get the lock on this task
                var db = _getRedisDb();
                var name = task.Name;
                var lockAcquired = await db.ScriptEvaluateAsync(s_redisLockScript, new { lockKey, host, px, lastKey, name, lastLockValue }).ConfigureAwait(false);

                if ((int)lockAcquired == 1)
                {
                    // we got the lock, now run the task
                    if (originalCallback != null)
                        originalCallback(task, timeIntendedToRun);
                    else
                        await originalAsyncCallback(task, timeIntendedToRun).ConfigureAwait(false);
                }
            };
        }
    }
}
