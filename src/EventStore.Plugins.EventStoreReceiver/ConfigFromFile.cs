using System;
using System.IO;
using EventStore.Plugins.EventStoreReceiver.Config;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreReceiver
{
    public class ConfigFromFile : IConfigProvider
    {
        private static readonly Serilog.ILogger Log = Serilog.Log.ForContext<ConfigFromFile>();
        private readonly string _configPath;

        public ConfigFromFile(string configPath)
        {
            _configPath = configPath;
        }

        public Root GetSettings()
        {
            try
            {
                var jsonFile = File.ReadAllText(_configPath);
                var settingsData = JsonConvert.DeserializeObject<dynamic>(jsonFile);
                if (settingsData == null || settingsData.receiver == null || settingsData.receiver.name == null ||
                    settingsData.receiver.id == null)
                    return null;
                var port = 1113;
                if (settingsData.receiver.port != null && !int.TryParse(settingsData.receiver.port.ToString(), out port))
                    port = 1113;
                var origin = new Config.Receiver(settingsData.receiver.name.ToString(), settingsData.receiver.id.ToString(),
                    port, settingsData.receiver.username.ToString(), settingsData.receiver.password.ToString(),
                    settingsData.receiver.inputStream.ToString(), bool.Parse(settingsData.receiver.appendInCaseOfConflict.ToString()));
                return new Root(origin);
            }
            catch (FileNotFoundException e)
            {
                Log.Information("EventStoreReceiver Configuration file not found");
            }
            catch (Exception ex)
            {
                Log.Error($"EventStoreReceiver Unhandled exception while reading configuration file: {ex.GetBaseException().Message}");
            }
            return null;
        }
    }
}
