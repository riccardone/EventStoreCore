using System;
using System.ComponentModel.Composition;
using System.IO;
using EventStore.Plugins.Dispatcher;
using Serilog;

namespace EventStore.Plugins.EventStoreDispatcher
{
    [Export(typeof(IDispatcherPlugin))]
    public class DispatcherPlugin : IDispatcherPlugin
    {
        public string Name => "EventStore Dispatcher Plugin";
        public string Version => "1.0";

        public DispatcherPlugin()
        {
            ConfigureLogging();
        }

        public IDispatcherServiceFactory GetStrategyFactory()
        {
            var root = new ConfigFromFile(Path.Combine(Environment.CurrentDirectory, "plugins",
                "EventStoreDispatcherPlugin", "config.json")).GetSettings();
            return new DispatcherServiceFactory(root, new DispatcherFactory(root));
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
