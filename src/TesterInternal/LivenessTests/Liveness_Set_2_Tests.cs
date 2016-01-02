//#define USE_SQL_SERVER
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using UnitTests.GrainInterfaces;
using UnitTests.Tester;
using UnitTests.TestHelper;

#pragma warning disable 618

namespace UnitTests.LivenessTests
{
    public abstract class Liveness_Set_2_Base : UnitTestSiloHost
    {
        private const int numAdditionalSilos = 1;
        private const int numGrains = 100;

        public TestContext TestContext { get; set; }

        private readonly TraceLogger logger = TraceLogger.GetLogger("LivenessTests", TraceLogger.LoggerType.Application);

        protected Liveness_Set_2_Base(TestingSiloOptions siloOptions, TestingClientOptions clientOptions)
            : base(siloOptions, clientOptions)
        { }

        protected void DoTestCleanup()
        {
            Console.WriteLine("Test {0} completed - Outcome = {1}", TestContext.TestName, TestContext.CurrentTestOutcome);

            StopAllSilos();
        }

        protected async Task Liveness_Set_2_Runner(int silo2Stop, bool softKill = true, bool startTimers = false)
        {
            List<SiloHandle> additionalSilos = StartAdditionalSilos(numAdditionalSilos);
            await WaitForLivenessToStabilizeAsync();

            List<ITestGrain> grains = new List<ITestGrain>();
            for (int i = 0; i < numGrains; i++)
            {
                long key = i + 1;
                ITestGrain g1 = GrainClient.GrainFactory.GetGrain<ITestGrain>(key);
                grains.Add(g1);
                Assert.AreEqual(key, g1.GetPrimaryKeyLong());
                //Assert.AreEqual(key, g1.GetKey().Result);
                Assert.AreEqual(key.ToString(CultureInfo.InvariantCulture), await g1.GetLabel());
                if (startTimers)
                {
                    await g1.StartTimer();
                }
                await LogGrainIdentity(logger, g1);
            }

            SiloHandle silo2Kill;
            if (silo2Stop == 0)
                silo2Kill = Primary;
            else if (silo2Stop == 1)
                silo2Kill = Secondary;
            else
                silo2Kill = additionalSilos[silo2Stop - 2];

            logger.Info("\n\n\n\nAbout to kill {0}\n\n\n", silo2Kill.Endpoint);

            if (softKill)
                StopSilo(silo2Kill);
            else
                KillSilo(silo2Kill);

            await WaitForLivenessToStabilizeAsync(softKill);

            logger.Info("\n\n\n\nAbout to start sending msg to grain again\n\n\n");

            for (int i = 0; i < grains.Count; i++)
            {
                long key = i + 1;
                ITestGrain g1 = grains[i];
                Assert.AreEqual(key, g1.GetPrimaryKeyLong());
                Assert.AreEqual(key.ToString(CultureInfo.InvariantCulture), await g1.GetLabel());
                await LogGrainIdentity(logger, g1);
            }

            for (int i = numGrains; i < 2 * numGrains; i++)
            {
                long key = i + 1;
                ITestGrain g1 = GrainClient.GrainFactory.GetGrain<ITestGrain>(key);
                grains.Add(g1);
                Assert.AreEqual(key, g1.GetPrimaryKeyLong());
                Assert.AreEqual(key.ToString(CultureInfo.InvariantCulture), await g1.GetLabel());
                await LogGrainIdentity(logger, g1);
            }
            logger.Info("======================================================");
        }

        private static async Task LogGrainIdentity(TraceLogger logger, ITestGrain grain)
        {
            logger.Info("Grain {0}, activation {1} on {2}",
                await grain.GetGrainReference(),
                await grain.GetActivationId(),
                await grain.GetRuntimeInstanceId());
        }
    }

    [TestClass]
    [DeploymentItem("OrleansConfigurationForTesting.xml")]
    [DeploymentItem("ClientConfigurationForTesting.xml")]
    public class Liveness_Set_2_MembershipGrain : Liveness_Set_2_Base
    {
        private static readonly TestingSiloOptions siloOptions = new TestingSiloOptions
        {
            StartFreshOrleans = true,
            StartPrimary = true,
            StartSecondary = true,
            LivenessType = GlobalConfiguration.LivenessProviderType.MembershipTableGrain,
            ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.ReminderTableGrain,
            SiloConfigFile = new FileInfo("OrleansConfigurationForTesting.xml")
        };
        private static readonly TestingClientOptions clientOptions = new TestingClientOptions
        {
            ProxiedGateway = true,
            Gateways = new List<IPEndPoint>(new[]
            {
                new IPEndPoint(IPAddress.Loopback, 40000), 
                new IPEndPoint(IPAddress.Loopback, 40001)
            }),
            PreferedGatewayIndex = 1,
            ClientConfigFile = new FileInfo("ClientConfigurationForTesting.xml")
        };

        public Liveness_Set_2_MembershipGrain()
            : base(siloOptions, clientOptions)
        { }

        [TestCleanup]
        public void TestCleanup()
        {
            base.DoTestCleanup();
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness")]
        public async Task Liveness_Grain_Set_2_Kill_GW()
        {
            await Liveness_Set_2_Runner(1);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness")]
        public async Task Liveness_Grain_Set_2_Kill_Silo_1()
        {
            await Liveness_Set_2_Runner(2);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness")]
        public async Task Liveness_Grain_Set_2_Kill_Silo_1_With_Timers()
        {
            await Liveness_Set_2_Runner(2, false, true);
        }
    }

    [TestClass]
    [DeploymentItem("OrleansConfigurationForTesting.xml")]
    [DeploymentItem("ClientConfigurationForTesting.xml")]
    public class Liveness_Set_2_AzureTable : Liveness_Set_2_Base
    {
        private static readonly TestingSiloOptions siloOptions = new TestingSiloOptions
        {
            StartFreshOrleans = true,
            StartPrimary = true,
            StartSecondary = true,
            DataConnectionString = StorageTestConstants.DataConnectionString,
            LivenessType = GlobalConfiguration.LivenessProviderType.AzureTable,
            ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.ReminderTableGrain,
            SiloConfigFile = new FileInfo("OrleansConfigurationForTesting.xml")
        };
        private static readonly TestingClientOptions clientOptions = new TestingClientOptions
        {
            ProxiedGateway = true,
            Gateways = new List<IPEndPoint>(new[]
            {
                new IPEndPoint(IPAddress.Loopback, 40000), 
                new IPEndPoint(IPAddress.Loopback, 40001)
            }),
            PreferedGatewayIndex = 1,
            ClientConfigFile = new FileInfo("ClientConfigurationForTesting.xml")
        };

        public Liveness_Set_2_AzureTable()
            : base(siloOptions, clientOptions)
        { }

        [TestCleanup]
        public void TestCleanup()
        {
            base.DoTestCleanup();
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness"), TestCategory("Azure")]
        public async Task Liveness_Azure_Set_2_Kill_Primary()
        {
            await Liveness_Set_2_Runner(0);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness"), TestCategory("Azure")]
        public async Task Liveness_Azure_Set_2_Kill_GW()
        {
            await Liveness_Set_2_Runner(1);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness"), TestCategory("Azure")]
        public async Task Liveness_Azure_Set_2_Kill_Silo_1()
        {
            await Liveness_Set_2_Runner(2);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness"), TestCategory("Azure")]
        public async Task Liveness_Azure_Set_2_Kill_Silo_1_With_Timers()
        {
            await Liveness_Set_2_Runner(2, false, true);
        }
    }

#if USE_SQL_SERVER || DEBUG
    [TestClass]
    [DeploymentItem("OrleansConfigurationForTesting.xml")]
    [DeploymentItem("ClientConfigurationForTesting.xml")]
    public class Liveness_Set_2_SqlServer : Liveness_Set_2_Base
    {
        private static readonly TestingSiloOptions siloOptions = new TestingSiloOptions
        {
            StartFreshOrleans = true,
            StartPrimary = true,
            StartSecondary = true,
            DataConnectionString = "Set-in-ClassInitialize",
            LivenessType = GlobalConfiguration.LivenessProviderType.SqlServer,
            ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.ReminderTableGrain,
            SiloConfigFile = new FileInfo("OrleansConfigurationForTesting.xml")
        };
        private static readonly TestingClientOptions clientOptions = new TestingClientOptions
        {
            ProxiedGateway = true,
            Gateways = new List<IPEndPoint>(new[]
            {
                new IPEndPoint(IPAddress.Loopback, 40000), 
                new IPEndPoint(IPAddress.Loopback, 40001)
            }),
            PreferedGatewayIndex = 1,
            ClientConfigFile = new FileInfo("ClientConfigurationForTesting.xml")
        };

        public Liveness_Set_2_SqlServer()
            : base(siloOptions, clientOptions)
        { }

        [ClassInitialize]
        public static void ClassInitialize(TestContext context)
        {
            Console.WriteLine("TestContext.DeploymentDirectory={0}", context.DeploymentDirectory);
            Console.WriteLine("TestContext=");
            Console.WriteLine(DumpTestContext(context));

            siloOptions.DataConnectionString = TestUtils.GetSqlConnectionString(context);

            ClientConfiguration cfg = ClientConfiguration.LoadFromFile("ClientConfigurationForTesting.xml");
            TraceLogger.Initialize(cfg);
#if DEBUG
            TraceLogger.AddTraceLevelOverride("Storage", Severity.Verbose3);
            TraceLogger.AddTraceLevelOverride("Membership", Severity.Verbose3);
#endif
        }

        [TestCleanup]
        public void TestCleanup()
        {
            base.DoTestCleanup();
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness"), TestCategory("SqlServer")]
        public async Task Liveness_Sql_Set_2_Kill_Primary()
        {
            await Liveness_Set_2_Runner(0);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness"), TestCategory("SqlServer")]
        public async Task Liveness_Sql_Set_2_Kill_GW()
        {
            await Liveness_Set_2_Runner(1);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness"), TestCategory("SqlServer")]
        public async Task Liveness_Sql_Set_2_Kill_Silo_1()
        {
            await Liveness_Set_2_Runner(2);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Liveness"), TestCategory("SqlServer")]
        public async Task Liveness_Sql_Set_2_Kill_Silo_1_With_Timers()
        {
            await Liveness_Set_2_Runner(2, false, true);
        }
    }
#endif
}

#pragma warning restore 618
