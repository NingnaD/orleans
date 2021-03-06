﻿using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Scheduler;

namespace UnitTests
{
    public class TestHelper
    {
        internal static OrleansTaskScheduler InitializeSchedulerForTesting(ISchedulingContext context)
        {
            StatisticsCollector.StatisticsCollectionLevel = StatisticsLevel.Info;
            SchedulerStatisticsGroup.Init();
            var scheduler = new OrleansTaskScheduler(4);
            scheduler.Start();
            WorkItemGroup ignore = scheduler.RegisterWorkContext(context);
            return scheduler;
        }
    }
}
