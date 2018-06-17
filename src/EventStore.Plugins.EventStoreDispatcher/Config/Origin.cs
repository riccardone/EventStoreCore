namespace EventStore.Plugins.EventStoreDispatcher.Config
{
    public class Origin
    {
        public Origin(string name, string id)
        {
            Name = name;
            Id = id;
        }

        public string Name { get; }
        public string Id { get; }

        public override string ToString()
        {
            return $"{Name}-{Id}";
        }
    }
}
