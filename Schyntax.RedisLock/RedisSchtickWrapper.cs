using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Schyntax.RedisLock
{
    public class RedisSchtickWrapper
    {
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

        public DateTime GetLastRunTime(string taskName)
        {
            var db = _getRedisDb();
            var value = db.HashGet(_lastKey, taskName);
            return LastRunValueToDate(value);
        }

        public async Task<DateTime> GetLastRunTimeAsync(string taskName)
        {
            var db = _getRedisDb();
            var value = await db.HashGetAsync(_lastKey, taskName);
            return LastRunValueToDate(value);
        }

        private static DateTime LastRunValueToDate(RedisValue value)
        {
            DateTime lastRun = default(DateTime);
            if (value.HasValue)
            {
                string str = value;
                var i = str.IndexOf(';');
                if (i != -1)
                {
                    DateTime.TryParse(str.Substring(0, i), out lastRun);
                }
            }

            return lastRun;
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

                // see if we can get the lock on this task
                var db = _getRedisDb();
                var tran = db.CreateTransaction();
                tran.AddCondition(Condition.KeyNotExists(lockKey));

#pragma warning disable 4014 // disable warning about not awaiting these tasks
                tran.StringSetAsync(lockKey, host, expiry);
                tran.HashSetAsync(lastKey, task.Name, lastLockValue);
#pragma warning restore 4014

                var committed = await tran.ExecuteAsync();

                if (committed)
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
