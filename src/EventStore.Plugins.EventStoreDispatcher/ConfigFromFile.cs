using System;
using System.Collections.Generic;
using System.IO;
using EventStore.Plugins.EventStoreDispatcher.Config;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreDispatcher
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
            var destinations = new List<Destination>();
            var jsonFile = File.ReadAllText(_configPath);
            var settingsData = JsonConvert.DeserializeObject<dynamic>(jsonFile);
            var origin = new Origin(settingsData.origin.name.ToString(), settingsData.origin.id.ToString());
            foreach (var setting in settingsData.destinations)
            {
                destinations.Add(new Destination(setting.name.ToString(),
                    setting.id.ToString(), new Uri(setting.connectionString.ToString())));
            }
            return new Root(origin, destinations);
        }
    }
}
