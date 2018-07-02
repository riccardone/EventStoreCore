using System;
using System.Collections.Generic;
using System.IO;
using EventStore.Plugins.GeoReplica.Config;
using Newtonsoft.Json;

namespace EventStore.Plugins.GeoReplica
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
                if (settingsData == null) 
                    return null;
                var port = 1113;
                if (settingsData.localInstance.port != null && !int.TryParse(settingsData.localInstance.port.ToString(), out port))
                    port = 1113;
                var localInstance = new LocalInstance(settingsData.localInstance.name.ToString(), settingsData.localInstance.id.ToString(), port,
                    settingsData.localInstance.username.ToString(), settingsData.localInstance.password.ToString());
                var destinations = new List<Destination>();
                if (settingsData.destinations != null)
                {
                    foreach (var setting in settingsData.destinations)
                    {
                        destinations.Add(new Destination(setting.name.ToString(),
                            setting.id.ToString(), new Uri(setting.connectionString.ToString()),
                            setting.inputStream.ToString(), int.Parse(setting.interval.ToString()),
                            int.Parse(setting.batchSize.ToString()), setting.username?.ToString(), setting.password?.ToString()));
                    }
                }
                var receivers = new List<Config.Receiver>();
                if (settingsData.receivers != null)
                {
                    foreach (var receiver in settingsData.receivers)
                    {
                        receivers.Add(new Config.Receiver(localInstance.Name,
                            localInstance.Id, localInstance.User, localInstance.Password,
                            receiver.inputStream.ToString(),
                            bool.Parse(receiver.appendInCaseOfConflict.ToString()),
                            int.Parse(receiver.checkpointInterval.ToString())));
                    }
                }

                if (destinations.Count == 0 && receivers.Count == 0)
                    return null;
                return new Root(localInstance, destinations, receivers);
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
