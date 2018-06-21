using System.IO;
using EventStore.Plugins.EventStoreReceiver.Config;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreReceiver
{
    public class ConfigFromFile : IConfigProvider
    {
        private readonly string _configPath;

        public ConfigFromFile(string configPath)
        {
            _configPath = configPath;
        }

        public Root GetSettings()
        {
            var jsonFile = File.ReadAllText(_configPath);
            var settingsData = JsonConvert.DeserializeObject<dynamic>(jsonFile);
            var port = 1113;
            if (settingsData.receiver.port != null && !int.TryParse(settingsData.receiver.port.ToString(), out port))
                port = 1113;
            var origin = new Config.Receiver(settingsData.receiver.name.ToString(), settingsData.receiver.id.ToString(),
                port, settingsData.receiver.username.ToString(), settingsData.receiver.password.ToString(),
                settingsData.receiver.inputStream.ToString());
            return new Root(origin);
        }
    }
}
