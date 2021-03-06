using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.AzureUtils;
using Orleans.TestingHost;

namespace UnitTests.StorageTests
{
    [TestClass]
    public class AzureQueueDataManagerTests
    {
        private readonly TraceLogger logger;
        public static string DeploymentId = "aqdatamanagertests".ToLower();
        private string queueName;

        public AzureQueueDataManagerTests()
        {
            ClientConfiguration config = new ClientConfiguration();
            config.TraceFilePattern = null;
            TraceLogger.Initialize(config);
            logger = TraceLogger.GetLogger("AzureQueueDataManagerTests", TraceLogger.LoggerType.Application);
        }

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            //Starts the storage emulator if not started already and it exists (i.e. is installed).
            if(!StorageEmulator.TryStart())
            {
                Console.WriteLine("Azure Storage Emulator could not be started.");
            }
        }

        [TestCleanup]
        public void TestCleanup()
        {
            AzureQueueDataManager manager = GetTableManager(queueName).Result;
            manager.DeleteQueue().Wait();
        }


        private async Task<AzureQueueDataManager> GetTableManager(string qName)
        {
            AzureQueueDataManager manager = new AzureQueueDataManager(qName, DeploymentId, StorageTestConstants.DataConnectionString);
            await manager.InitQueueAsync();
            return manager;
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Azure"), TestCategory("Storage"), TestCategory("AzureQueue")]
        public async Task AQ_Standalone_1()
        {
            queueName = "Test-1-".ToLower() + Guid.NewGuid();
            AzureQueueDataManager manager = await GetTableManager(queueName);
            Assert.AreEqual(0, await manager.GetApproximateMessageCount());

            CloudQueueMessage inMessage = new CloudQueueMessage("Hello, World");
            await manager.AddQueueMessage(inMessage);
            //Nullable<int> count = manager.ApproximateMessageCount;
            Assert.AreEqual(1, await manager.GetApproximateMessageCount());

            CloudQueueMessage outMessage1 = await manager.PeekQueueMessage();
            logger.Info("PeekQueueMessage 1: {0}", AzureStorageUtils.PrintCloudQueueMessage(outMessage1));
            Assert.AreEqual(inMessage.AsString, outMessage1.AsString);

            CloudQueueMessage outMessage2 = await manager.PeekQueueMessage();
            logger.Info("PeekQueueMessage 2: {0}", AzureStorageUtils.PrintCloudQueueMessage(outMessage2));
            Assert.AreEqual(inMessage.AsString, outMessage2.AsString);

            CloudQueueMessage outMessage3 = await manager.GetQueueMessage();
            logger.Info("GetQueueMessage 3: {0}", AzureStorageUtils.PrintCloudQueueMessage(outMessage3));
            Assert.AreEqual(inMessage.AsString, outMessage3.AsString);
            Assert.AreEqual(1, await manager.GetApproximateMessageCount());

            CloudQueueMessage outMessage4 = await manager.GetQueueMessage();
            Assert.IsNull(outMessage4);

            Assert.AreEqual(1, await manager.GetApproximateMessageCount());

            await manager.DeleteQueueMessage(outMessage3);
            Assert.AreEqual(0, await manager.GetApproximateMessageCount());
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Azure"), TestCategory("Storage"), TestCategory("AzureQueue")]
        public async Task AQ_Standalone_2()
        {
            queueName = "Test-2-".ToLower() + Guid.NewGuid();
            AzureQueueDataManager manager = await GetTableManager(queueName);

            IEnumerable<CloudQueueMessage> msgs = await manager.GetQueueMessages();
            Assert.IsTrue(msgs == null || msgs.Count() == 0);

            int numMsgs = 10;
            List<Task> promises = new List<Task>();
            for (int i = 0; i < numMsgs; i++)
            {
                promises.Add(manager.AddQueueMessage(new CloudQueueMessage(i.ToString())));
            }
            Task.WaitAll(promises.ToArray());
            Assert.AreEqual(numMsgs, await manager.GetApproximateMessageCount());

            msgs = new List<CloudQueueMessage>(await manager.GetQueueMessages(numMsgs));
            Assert.AreEqual(numMsgs, msgs.Count());
            Assert.AreEqual(numMsgs, await manager.GetApproximateMessageCount());

            promises = new List<Task>();
            foreach (var msg in msgs)
            {
                promises.Add(manager.DeleteQueueMessage(msg));
            }
            Task.WaitAll(promises.ToArray());
            Assert.AreEqual(0, await manager.GetApproximateMessageCount());
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Azure"), TestCategory("Storage"), TestCategory("AzureQueue")]
        public async Task AQ_Standalone_3_Init_MultipleThreads()
        {
            queueName = "Test-4-".ToLower() + Guid.NewGuid();

            const int NumThreads = 100;
            Task<bool>[] promises = new Task<bool>[NumThreads];

            for (int i = 0; i < NumThreads; i++) 
            {
                promises[i] = Task.Run<bool>(async () =>
                {
                    AzureQueueDataManager manager = await GetTableManager(queueName);
                    return true;
                });
            }
            await Task.WhenAll(promises);
        }
    }
}
