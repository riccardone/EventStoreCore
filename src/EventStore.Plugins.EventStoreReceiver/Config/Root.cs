using System.Collections.Generic;

namespace EventStore.Plugins.EventStoreReceiver.Config
{
    public class Root
    {
        public Root(Local local, List<Receiver> receivers)
        {
            Local = local;
            Receivers = receivers;
        }

        public Local Local { get; }
        public List<Receiver> Receivers { get; }
    }
}
