using System.IO;
using System.Reflection;
using Newtonsoft.Json;

namespace PusherServer.Tests.Helpers
{
    /// <summary>
    /// Loads test configuration from a json file.
    /// </summary>
    public class JsonFileConfigLoader : IApplicationConfigLoader
    {
        private const string DefaultFileName = "AppConfig.test.json";

        public JsonFileConfigLoader()
            : this(Path.Combine(Assembly.GetExecutingAssembly().Location, $"../../../../../{DefaultFileName}"))
        {
        }

        public JsonFileConfigLoader(string fileName)
        {
            FileName = fileName;
        }

        public static IApplicationConfigLoader Default { get; } = new JsonFileConfigLoader();

        public string FileName { get; set; }

        public IApplicationConfig Load()
        {
            FileInfo fileInfo = new FileInfo(FileName);
            if (!fileInfo.Exists)
            {
                return new ApplicationConfig();
            }

            string content = File.ReadAllText(fileInfo.FullName);
            ApplicationConfig result = JsonConvert.DeserializeObject<ApplicationConfig>(content);
            return result ?? new ApplicationConfig();
        }
    }
}
