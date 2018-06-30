using System;
using System.Collections.Generic;
using System.IO;
using EventStore.Plugins.EventStoreDispatcher.Config;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreDispatcher
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
                if (settingsData == null || settingsData.destinations == null || settingsData.destinations.Count == 0)
                    return null;
                var port = 1113;
                if (settingsData.origin.port != null && !int.TryParse(settingsData.origin.port.ToString(), out port))
                    port = 1113;
                var origin = new Origin(settingsData.origin.name.ToString(), settingsData.origin.id.ToString(), port,
                    settingsData.origin.username.ToString(), settingsData.origin.password.ToString());
                var destinations = new List<Destination>();
                foreach (var setting in settingsData.destinations)
                {
                    destinations.Add(new Destination(setting.name.ToString(),
                        setting.id.ToString(), new Uri(setting.connectionString.ToString()),
                        setting.inputStream.ToString(), int.Parse(setting.interval.ToString()),
                        int.Parse(setting.batchSize.ToString())));
                }
                return new Root(origin, destinations);
            }
            catch (FileNotFoundException)
            {
                //Log.Information("EventStoreDispatcher Configuration file not found");
            }
            catch (Exception ex)
            {
                Log.Error($"EventStoreDispatcher Unhandled exception while reading configuration file: {ex.GetBaseException().Message}");
            }
            return null;
        }
    }
}
