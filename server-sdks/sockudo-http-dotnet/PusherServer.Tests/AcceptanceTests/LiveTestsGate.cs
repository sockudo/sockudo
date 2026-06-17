using NUnit.Framework;

namespace PusherServer.Tests.AcceptanceTests
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
