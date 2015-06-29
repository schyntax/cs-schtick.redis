using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using StackExchange.Redis;

namespace Schyntax.RedisLock.Tests
{
    [TestFixture]
    public class Tests
    {
        [Test]
        public void BasicTest()
        {
            var redis = ConnectionMultiplexer.Connect("localhost:6379");
            var schticks = CreateSchtickArray(4, () => redis.GetDatabase());

            var locker = new object();
            var runs = new HashSet<DateTime>();
            var runCount = 0;

            // add a task and make sure it only executes on one "server"
            var tasks = AddTaskMulti("basic", "sec(*)", schticks, (task, run) =>
            {
                lock (locker)
                {
                    runs.Add(run);
                    runCount++;
                }
            });

            Thread.Sleep(3000);

            foreach (var t in tasks)
            {
                t.Stop();
            }

            Assert.IsTrue(runs.Count == runCount, $"runs.Count ({runs.Count}) was expected to be equal to runCount ({runCount}).");
            Assert.IsTrue(runs.Count >= 2, $"runs.Count was {runs.Count}. Expected at least 2.");
            Assert.IsTrue(runs.Count <= 3, $"runs.Count was {runs.Count}. Expected no more than 3.");
        }

        private static RedisLockedSchtick[] CreateSchtickArray(int count, Func<IDatabase> getDb)
        {
            var schticks = new RedisLockedSchtick[count];
            for (var i = 0; i < count; i++)
            {
                schticks[i] = new RedisLockedSchtick(getDb, "test-" + i);
            }

            return schticks;
        }

        private static ScheduledTask[] AddTaskMulti(string name, string schedule, RedisLockedSchtick[] schticks, ScheduledTaskCallback callback)
        {
            var tasks = new ScheduledTask[schticks.Length];
            for (var i = 0; i < schticks.Length; i++)
            {
                 tasks[i] = schticks[i].AddTask(name, schedule, callback);
            }

            return tasks;
        }
    }
}
