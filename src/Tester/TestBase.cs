using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTests.Tester
{
    /// <summary>
    /// This class declares all the additional deployment items required by tests 
    /// - such as the TestGrain assemblies, the client and server config files.
    /// </summary>
    [DeploymentItem("OrleansConfigurationForTesting.xml")]
    [DeploymentItem("ClientConfigurationForTesting.xml")]
    [DeploymentItem("TestGrainInterfaces.dll")]
    [DeploymentItem("TestGrains.dll")]
    [DeploymentItem("TestInternalGrainInterfaces.dll")]
    [DeploymentItem("TestInternalGrains.dll")]
    public class TestBase
    {

    }
}
