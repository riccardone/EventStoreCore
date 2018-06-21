using System;
using System.ComponentModel.Composition;
using System.IO;
using EventStore.Plugins.Receiver;
using Serilog;

namespace EventStore.Plugins.EventStoreReceiver
{
    [Export(typeof(IReceiverPlugin))]
    public class ReceiverPlugin : IReceiverPlugin
    {
        public string Name => "EventStore Receiver Plugin";
        public string Version => "1.0";

        public ReceiverPlugin()
        {
            ConfigureLogging();
        }

        public IReceiverServiceFactory GetStrategyFactory()
        {
            var root = new ConfigFromFile(Path.Combine(Environment.CurrentDirectory, "plugins",
                "EventStoreReceiverPlugin", "config.json")).GetSettings();
            return new ReceiverServiceFactory(root);
        }

        private static void ConfigureLogging()
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console(
                    outputTemplate:
                    "[{Timestamp:HH:mm:ss} {Level:u3} {SourceContext:l}] {Message:lj}{NewLine}{Exception}")
                .WriteTo.File("logs\\plugins\\EventStoreReceiver\\receiver.log", rollingInterval: RollingInterval.Day,
                    outputTemplate:
                    "[{Timestamp:HH:mm:ss} {Level:u3} {SourceContext:l}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger();
        }
    }
}
