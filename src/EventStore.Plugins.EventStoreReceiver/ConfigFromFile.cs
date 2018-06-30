using System;
using System.Collections.Generic;
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
                if (settingsData == null || settingsData.local == null || settingsData.receivers == null ||
                    settingsData.receivers.Count == 0)
                    return null;
                var port = 1113;
                if (settingsData.local.port != null && !int.TryParse(settingsData.local.port.ToString(), out port))
                    port = 1113;
                var local = new Local(settingsData.local.name.ToString(), port);
                var receivers = new List<Config.Receiver>();
                foreach (var receiver in settingsData.receivers)
                {
                    receivers.Add(new Config.Receiver(local.Name,
                        receiver.id.ToString(), receiver.username.ToString(), receiver.password.ToString(),
                        receiver.inputStream.ToString(),
                        bool.Parse(receiver.appendInCaseOfConflict.ToString()),
                        int.Parse(receiver.checkpointInterval.ToString())));
                }
                return new Root(local, receivers);
            }
            catch (FileNotFoundException)
            {
                //Log.Information("EventStoreReceiver Configuration file not found");
            }
            catch (Exception ex)
            {
                Log.Error($"EventStoreReceiver Unhandled exception while reading configuration file: {ex.GetBaseException().Message}");
            }
            return null;
        }
    }
}
