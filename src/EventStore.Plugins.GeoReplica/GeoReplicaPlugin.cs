using System;
using System.ComponentModel.Composition;
using System.IO;
using Serilog;

namespace EventStore.Plugins.GeoReplica
{
    [Export(typeof(IEventStorePlugin))]
    public class GeoReplicaPlugin : IEventStorePlugin
    {
        public string Name => "EventStore GeoReplica Plugin";
        public string Version => "1.0";

        public GeoReplicaPlugin()
        {
            ConfigureLogging();
        }

        public IEventStoreServiceFactory GetStrategyFactory()
        {
            var root = new ConfigFromFile(Path.Combine(Environment.CurrentDirectory, "plugins",
                "GeoReplicaPlugin", "config.json")).GetSettings();
            return new GeoReplicaServiceFactory(root);
        }

        private static void ConfigureLogging()
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console(
                    outputTemplate:
                    "[{Timestamp:HH:mm:ss} {Level:u3} {SourceContext:l}] {Message:lj}{NewLine}{Exception}")
                .WriteTo.File("logs\\plugins\\EventStoreDispatcher\\dispatcher.log", rollingInterval: RollingInterval.Day,
                    outputTemplate:
                    "[{Timestamp:HH:mm:ss} {Level:u3} {SourceContext:l}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger();
        }
    }
}
