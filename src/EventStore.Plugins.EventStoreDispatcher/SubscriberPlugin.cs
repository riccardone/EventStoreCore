using System;
using System.ComponentModel.Composition;
using System.IO;
using EventStore.Plugins.Dispatcher;

namespace EventStore.Plugins.EventStoreDispatcher
{
    [Export(typeof(IDispatcherPlugin))]
    public class SubscriberPlugin : IDispatcherPlugin
    {
        public string Name => "EventStore Subscriber Plugin";
        public string Version => "1.0";
        public ISubscriberServiceFactory GetStrategyFactory()
        {
            var root = new ConfigFromFile(Path.Combine(Environment.CurrentDirectory, "plugins",
                "EventStoreDispatcherPlugin", "config.json")).GetSettings();
            return new SubscriberServiceFactory(root, new DispatcherFactory(root));
        }
    }
}
