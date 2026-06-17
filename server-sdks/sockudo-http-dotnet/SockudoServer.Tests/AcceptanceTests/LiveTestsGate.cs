using NUnit.Framework;

namespace SockudoServer.Tests.AcceptanceTests
{
    [SetUpFixture]
    public class LiveTestsGate
    {
        [OneTimeSetUp]
        public void SkipWhenLiveConfigIsMissing()
        {
            if (!Config.IsConfigured)
            {
                Assert.Ignore(Config.SkipReason);
            }
        }
    }
}
