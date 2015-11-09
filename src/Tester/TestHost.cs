using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.TestingHost;
using Orleans.Runtime.Configuration;

namespace UnitTests.Tester
{
    public class TestHost : TestingSiloHost, IDisposable
    {
        public TestHost() { }

        public TestHost(bool startFreshOrleans) { }

        public TestHost(TestingSiloOptions siloOptions) : base(siloOptions) { }

        public TestHost(TestingSiloOptions siloOptions, TestingClientOptions clientOptions) : base(siloOptions, clientOptions) { }

        private Action<ClusterConfiguration> adjustClusterConfig;
        private Action<ClientConfiguration> adjustClientConfig;

        public void Dispose()
        {
            StopAllSilos();
        }
    }
}
