using System.Collections.Generic;

namespace EventStore.Plugins.EventStoreDispatcher.Config
{
    public class Root
    {
        public Root(Origin origin, List<Destination> destinations)
        {
            Origin = origin;
            Destinations = destinations;
        }

        public Origin Origin { get; }
        public List<Destination> Destinations { get; }
    }
}
