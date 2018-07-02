using System.Collections.Generic;

namespace EventStore.Plugins.GeoReplica.Config
{
    public class Root
    {
        public Root(LocalInstance origin, List<Destination> destinations, List<Receiver> receivers)
        {
            LocalInstance = origin;
            Destinations = destinations;
            Receivers = receivers;
        }

        public LocalInstance LocalInstance { get; }
        public List<Destination> Destinations { get; }
        public List<Receiver> Receivers { get; }
    }
}
