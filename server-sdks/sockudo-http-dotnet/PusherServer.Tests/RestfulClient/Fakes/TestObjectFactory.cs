namespace PusherServer.Tests.RestfulClient.Fakes
{
    internal class TestObjectFactory
    {
        public MyTestObject Create(string property1Value, int property2Value, bool property3Value)
        {
            return new MyTestObject
            {
                Property1 = property1Value,
                Property2 = property2Value,
                Property3 = property3Value
            };
        }
    }
}