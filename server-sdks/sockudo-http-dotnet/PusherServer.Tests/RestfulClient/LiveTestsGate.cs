using NUnit.Framework;

namespace PusherServer.Tests.RestfulClient
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
