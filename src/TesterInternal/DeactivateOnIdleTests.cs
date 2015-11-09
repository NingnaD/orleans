using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Runtime;
using Orleans.TestingHost;
using TestGrains;
using TestInternalGrainInterfaces;
using UnitTests.Tester;

namespace UnitTests.ActivationsLifeCycleTests
{
    [TestClass]
    public class DeactivateOnIdleTests : TestBase
    {
        private static readonly TestingSiloOptions TestOptions = new TestingSiloOptions
        {
            StartFreshOrleans = true,
            StartSecondary = false
        };

        [TestMethod, TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task DeactivateOnIdleTestInside_Basic()
        {
            using (var testHost = new TestHost(TestOptions.Copy()))
            {
                var a = GrainClient.GrainFactory.GetGrain<ICollectionTestGrain>(1);
                var b = GrainClient.GrainFactory.GetGrain<ICollectionTestGrain>(2);
                await a.SetOther(b);
                await a.GetOtherAge(); // prime a's routing cache
                await b.DeactivateSelf();
                Thread.Sleep(5000);
                try
                {
                    var age = a.GetOtherAge().WaitForResultWithThrow(TimeSpan.FromMilliseconds(2000));
                    Assert.IsTrue(age.TotalMilliseconds < 2000, "Should be newly activated grain");
                }
                catch (TimeoutException)
                {
                    Assert.Fail("Should not time out when reactivating grain");
                }
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task DeactivateOnIdleTest_Stress_1()
        {
            using (var testHost = new TestHost(TestOptions.Copy()))
            {
                var a = GrainClient.GrainFactory.GetGrain<ICollectionTestGrain>(1);
                await a.GetAge();
                await a.DeactivateSelf();
                for (int i = 0; i < 30; i++)
                {
                    await a.GetAge();
                }
            }
        }

        [TestMethod, TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task DeactivateOnIdleTest_Stress_2_NonReentrant()
        {
            using (var testHost = new TestHost(TestOptions.Copy()))
            {
                var grainFullName = typeof(CollectionTestGrain).FullName;
                var a = GrainClient.GrainFactory.GetGrain<ICollectionTestGrain>(1, grainFullName);
                await a.IncrCounter();

                Task t1 = Task.Run(async () =>
                {
                    List<Task> tasks = new List<Task>();
                    for (int i = 0; i < 100; i++)
                    {
                        tasks.Add(a.IncrCounter());
                    }
                    await Task.WhenAll(tasks);
                });

                await Task.Delay(1);
                Task t2 = a.DeactivateSelf();
                await Task.WhenAll(t1, t2);
            }
        }

        [TestMethod, TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task DeactivateOnIdleTest_Stress_3_Reentrant()
        {
            using (var testHost = new TestHost(TestOptions.Copy()))
            {
                var grainFullName = typeof(ReentrantCollectionTestGrain).FullName;
                var a = GrainClient.GrainFactory.GetGrain<ICollectionTestGrain>(1, grainFullName);
                await a.IncrCounter();

                Task t1 = Task.Run(async () =>
                {
                    List<Task> tasks = new List<Task>();
                    for (int i = 0; i < 100; i++)
                    {
                        tasks.Add(a.IncrCounter());
                    }
                    await Task.WhenAll(tasks);
                });

                await Task.Delay(TimeSpan.FromMilliseconds(1));
                Task t2 = a.DeactivateSelf();
                await Task.WhenAll(t1, t2);
            }
        }

        [TestMethod, TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task DeactivateOnIdleTest_Stress_4_Timer()
        {
            using (var testHost = new TestHost(TestOptions.Copy()))
            {
                var grainFullName = typeof(ReentrantCollectionTestGrain).FullName;
                var a = GrainClient.GrainFactory.GetGrain<ICollectionTestGrain>(1, grainFullName);
                for (int i = 0; i < 10; i++)
                {
                    await a.StartTimer(TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(100));
                }
                await a.DeactivateSelf();
                await a.IncrCounter();
                //await Task.Delay(TimeSpan.FromSeconds(10));
            }
        }

        [TestMethod, TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task DeactivateOnIdleTest_Stress_5()
        {
            using (var testHost = new TestHost(TestOptions.Copy()))
            {
                var a = GrainClient.GrainFactory.GetGrain<ICollectionTestGrain>(1);
                await a.IncrCounter();

                Task t1 = Task.Run(async () =>
                {
                    List<Task> tasks = new List<Task>();
                    for (int i = 0; i < 100; i++)
                    {
                        tasks.Add(a.IncrCounter());
                    }
                    await Task.WhenAll(tasks);
                });
                Task t2 = Task.Run(async () =>
                {
                    List<Task> tasks = new List<Task>();
                    for (int i = 0; i < 1; i++)
                    {
                        await Task.Delay(1);
                        tasks.Add(a.DeactivateSelf());
                    }
                    await Task.WhenAll(tasks);
                });
                await Task.WhenAll(t1, t2);
            }
        }

        //[TestMethod, TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task DeactivateOnIdleTest_Stress_11()
        {
            using (var testHost = new TestHost(TestOptions.Copy()))
            {
                var a = GrainClient.GrainFactory.GetGrain<ICollectionTestGrain>(1);
                List<Task> tasks = new List<Task>();
                for (int i = 0; i < 100; i++)
                {
                    tasks.Add(a.IncrCounter());
                }
                await Task.WhenAll(tasks);
            }
        }

        [TestMethod, TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task DeactivateOnIdle_NonExistentActivation_1()
        {
            await DeactivateOnIdle_NonExistentActivation_Runner(0);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task DeactivateOnIdle_NonExistentActivation_2()
        {
            await DeactivateOnIdle_NonExistentActivation_Runner(1);
        }

        private async Task DeactivateOnIdle_NonExistentActivation_Runner(int forwardCount)
        {
            // Fix SiloGenerationNumber and later play with grain id to map grain id to the right directory partition.
            var options = new TestingSiloOptions
            {
                StartFreshOrleans = true,
                StartPrimary = true,
                StartSecondary = true,
                AdjustConfig = config =>
                {
                    config.Globals.MaxForwardCount = forwardCount;
                    foreach (var nodeConfig in config.Overrides.Values)
                    {
                        nodeConfig.Generation = 13;
                    }
                },
            };

            using (var testHost = new TestHost(options))
            {
                ICollectionTestGrain grain = await PickGrain(testHost);
                Assert.AreNotEqual(null, grain, "Could not create a grain that matched the desired requirements");

                testHost.logger.Info("About to make a 1st GetAge() call.");
                TimeSpan age = await grain.GetAge();
                testHost.logger.Info(age.ToString());

                await grain.DeactivateSelf();
                Thread.Sleep(3000);
                bool didThrow = false;
                bool didThrowCorrectly = false;
                Exception thrownException = null;
                try
                {
                    testHost.logger.Info("About to make a 2nd GetAge() call.");
                    age = await grain.GetAge();
                    testHost.logger.Info(age.ToString());
                }
                catch (Exception exc)
                {
                    didThrow = true;
                    thrownException = exc;
                    Exception baseException = exc.GetBaseException();
                    didThrowCorrectly = baseException.GetType().Equals(typeof(OrleansException)) && baseException.Message.Contains("Non-existent activation");
                }

                if (forwardCount == 0)
                {
                    Assert.IsTrue(didThrow, "The call did not throw exception as expected.");
                    Assert.IsTrue(didThrowCorrectly, "The call did not throw Non-existent activation Exception as expected. Instead it has thrown: " + thrownException);
                    testHost.logger.Info("\nThe 1st call after DeactivateSelf has thrown Non-existent activation exception as expected, since forwardCount is {0}.\n", forwardCount);
                }
                else
                {
                    Assert.IsFalse(didThrow, "The call has throw an exception, which was not expected. The exception is: " + (thrownException == null ? "" : thrownException.ToString()));
                    testHost.logger.Info("\nThe 1st call after DeactivateSelf has NOT thrown any exception as expected, since forwardCount is {0}.\n", forwardCount);
                }

                if (forwardCount == 0)
                {
                    didThrow = false;
                    thrownException = null;
                    // try sending agan now and see it was fixed.
                    try
                    {
                        age = await grain.GetAge();
                        testHost.logger.Info(age.ToString());
                    }
                    catch (Exception exc)
                    {
                        didThrow = true;
                        thrownException = exc;
                    }
                    Assert.IsFalse(didThrow, "The 2nd call has throw an exception, which was not expected. The exception is: " + (thrownException == null ? "" : thrownException.ToString()));
                    testHost.logger.Info("\nThe 2nd call after DeactivateSelf has NOT thrown any exception as expected, despite the fact that forwardCount is {0}, since we send CacheMgmtHeader.\n", forwardCount);
                }
            }
        }

        private async Task<ICollectionTestGrain> PickGrain(TestHost testHost)
        {
            for (int i = 0; i < 100; i++)
            {
                // Create grain such that:
                // Its directory owner is not the Gateway silo. This way Gateway will use its directory cache.
                // Its activation is located on the non Gateway silo as well.
                ICollectionTestGrain grain = GrainClient.GrainFactory.GetGrain<ICollectionTestGrain>(i);
                GrainId grainId = ((GrainReference)await grain.GetGrainReference()).GrainId;
                SiloAddress primaryForGrain = TestingSiloHost.Primary.Silo.LocalGrainDirectory.GetPrimaryForGrain(grainId);
                if (primaryForGrain.Equals(TestingSiloHost.Primary.Silo.SiloAddress))
                {
                    continue;
                }
                string siloHostingActivation = await grain.GetRuntimeInstanceId();
                if (TestingSiloHost.Primary.Silo.SiloAddress.ToLongString().Equals(siloHostingActivation))
                {
                    continue;
                }
                testHost.logger.Info("\nCreated grain with key {0} whose primary directory owner is silo {1} and which was activated on silo {2}\n", i, primaryForGrain.ToLongString(), siloHostingActivation);
                return grain;
            }
            return null;
        }

        [TestMethod, TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task MissingActivation_1()
        {
            using (var testHost = new TestHost(true))
            {
                for (int i = 0; i < 10; i++)
                {
                    await MissingActivation_Runner(i, false, testHost);
                }
            }
        }

        [TestMethod, TestCategory("Functional"), TestCategory("ActivationCollector")]
        public async Task MissingActivation_2()
        {
            using (var testHost = new TestHost(true))
            {
                for (int i = 0; i < 10; i++)
                {
                    await MissingActivation_Runner(i, true, testHost);
                }
            }
        }

        private async Task MissingActivation_Runner(int grainId, bool DoLazyDeregistration, TestHost testHost)
        {
            testHost.logger.Info("\n\n\n SMissingActivation_Runner.\n\n\n");

            IStressSelfManagedGrain g = GrainClient.GrainFactory.GetGrain<IStressSelfManagedGrain>(grainId);
            await g.SetLabel("hello_" + grainId);
            var grain = ((GrainReference)await g.GetGrainReference()).GrainId;

            // Call again to make sure the grain is in all silo caches
            for (int i = 0; i < 10; i++)
            {
                var label = await g.GetLabel();
            }

            TimeSpan LazyDeregistrationDelay;
            if (DoLazyDeregistration)
            {
                LazyDeregistrationDelay = TimeSpan.FromSeconds(2);
                // disable retries in this case, to make test more predictable.
                TestingSiloHost.Primary.Silo.TestHookup.SetMaxForwardCount_ForTesting(0);
                TestingSiloHost.Secondary.Silo.TestHookup.SetMaxForwardCount_ForTesting(0);
            }
            else
            {
                LazyDeregistrationDelay = TimeSpan.FromMilliseconds(-1);
                TestingSiloHost.Primary.Silo.TestHookup.SetMaxForwardCount_ForTesting(0);
                TestingSiloHost.Secondary.Silo.TestHookup.SetMaxForwardCount_ForTesting(0);
            }
            TestingSiloHost.Primary.Silo.TestHookup.SetDirectoryLazyDeregistrationDelay_ForTesting(LazyDeregistrationDelay);
            TestingSiloHost.Secondary.Silo.TestHookup.SetDirectoryLazyDeregistrationDelay_ForTesting(LazyDeregistrationDelay);

            // Now we know that there's an activation; try both silos and deactivate it incorrectly
            int primaryActivation = TestingSiloHost.Primary.Silo.TestHookup.UnregisterGrainForTesting(grain);
            int secondaryActivation = TestingSiloHost.Secondary.Silo.TestHookup.UnregisterGrainForTesting(grain);
            Assert.AreEqual(1, primaryActivation + secondaryActivation, "Test deactivate didn't find any activations");

            // If we try again, we shouldn't find any
            primaryActivation = TestingSiloHost.Primary.Silo.TestHookup.UnregisterGrainForTesting(grain);
            secondaryActivation = TestingSiloHost.Secondary.Silo.TestHookup.UnregisterGrainForTesting(grain);
            Assert.AreEqual(0, primaryActivation + secondaryActivation, "Second test deactivate found an activation");


            if (DoLazyDeregistration)
            {
                // Wait a bit
                TimeSpan pause = LazyDeregistrationDelay + TimeSpan.FromSeconds(1);
                testHost.logger.Info("Pausing for {0} because DoLazyDeregistration=True", pause);
                Thread.Sleep(pause);
            }

            //g1.DeactivateSelf().Wait();
            // Now send a message again; it should fail);
            try
            {
                var newLabel = await g.GetLabel();
                testHost.logger.Info("After 1nd call. newLabel = " + newLabel);
                Assert.Fail("First message after incorrect deregister should fail!");
            }
            catch (Exception exc)
            {
                testHost.logger.Info("Got 1st exception - " + exc.GetBaseException().Message);
                Exception baseExc = exc.GetBaseException();
                if (baseExc is AssertFailedException) throw;
                Assert.IsInstanceOfType(baseExc, typeof(OrleansException), "Unexpected exception type: " + baseExc);
                // Expected
                Assert.IsTrue(baseExc.Message.Contains("Non-existent activation"), "1st exception message");
                testHost.logger.Info("Got 1st Non-existent activation Exception, as expected.");
            }

            // Try again; it should succeed or fail, based on DoLazyDeregistration
            try
            {
                var newLabel = await g.GetLabel();
                testHost.logger.Info("After 2nd call. newLabel = " + newLabel);

                if (!DoLazyDeregistration)
                {
                    Assert.Fail("Exception should have been thrown when DoLazyDeregistration=False");
                }
            }
            catch (Exception exc)
            {
                testHost.logger.Info("Got 2nd exception - " + exc.GetBaseException().Message);
                if (DoLazyDeregistration)
                {
                    Assert.Fail("Second message after incorrect deregister failed, while it should have not! Exception=" + exc);
                }
                else
                {
                    Exception baseExc = exc.GetBaseException();
                    if (baseExc is AssertFailedException) throw;
                    Assert.IsInstanceOfType(baseExc, typeof(OrleansException), "Unexpected exception type: " + baseExc);
                    // Expected
                    Assert.IsTrue(baseExc.Message.Contains("duplicate activation") || baseExc.Message.Contains("Non-existent activation")
                               || baseExc.Message.Contains("Forwarding failed"),
                        "2nd exception message: " + baseExc);
                    testHost.logger.Info("Got 2nd exception, as expected.");
                }
            }
        }
    }
}
