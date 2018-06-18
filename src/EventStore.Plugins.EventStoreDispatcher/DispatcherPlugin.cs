//using System;
//using System.ComponentModel.Composition;
//using System.IO;
//using EventStore.Plugins.Dispatcher;

//namespace EventStore.Plugins.EventStoreDispatcher
//{
//    [Export(typeof(IDispatcherPlugin))]
//    public class DispatcherPlugin : IDispatcherPlugin
//    {
//        public string Name => "EventStore Dispatcher Plugin";
//        public string Version => "1.0";
//        public IDispatcherFactory GetStrategyFactory()
//        {
//            return new DispatcherFactory(new ConfigFromFile(Path.Combine(Environment.CurrentDirectory, "plugins",
//                "EventStoreDispatcherPlugin", "config.json")));
//        }
//    }
//}
