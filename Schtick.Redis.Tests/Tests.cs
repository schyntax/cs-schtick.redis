using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using StackExchange.Redis;

namespace Schyntax.Tests
{
    [TestFixture]
    public class Tests
    {
        private class Combo
        {
            public Schtick Schtick { get; set; }
            public RedisSchtickWrapper Wrapper { get; set; }
        }

        [Test]
        public void BasicTest()
        {
            var redis = ConnectionMultiplexer.Connect("localhost:6379");
            var combos = CreateCombosArray(4, () => redis.GetDatabase());

            var locker = new object();
            var runs = new HashSet<DateTimeOffset>();
            var runCount = 0;

            // add a task and make sure it only executes on one "server"
            var tasks = AddTaskMulti("basic", "sec(*)", combos, (task, run) =>
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
                t.StopSchedule();
            }

            Assert.IsTrue(runs.Count == runCount, $"runs.Count ({runs.Count}) was expected to be equal to runCount ({runCount}).");
            Assert.IsTrue(runs.Count >= 2, $"runs.Count was {runs.Count}. Expected at least 2.");
            Assert.IsTrue(runs.Count <= 3, $"runs.Count was {runs.Count}. Expected no more than 3.");
        }

        private static Combo[] CreateCombosArray(int count, Func<IDatabase> getDb)
        {
            var combos = new Combo[count];
            for (var i = 0; i < count; i++)
            {
                combos[i] = new Combo()
                {
                    Wrapper = new RedisSchtickWrapper(getDb, "test-" + i),
                    Schtick = new Schtick(),
                };
            }

            return combos;
        }

        private static ScheduledTask[] AddTaskMulti(string name, string schedule, Combo[] combos, ScheduledTaskCallback callback)
        {
            var tasks = new ScheduledTask[combos.Length];
            for (var i = 0; i < combos.Length; i++)
            {
                var schtick = combos[i].Schtick;
                var wrapper = combos[i].Wrapper;
                tasks[i] = schtick.AddAsyncTask(name, schedule, wrapper.Wrap(callback));
            }

            return tasks;
        }
    }
}
