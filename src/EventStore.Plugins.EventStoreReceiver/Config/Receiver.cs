namespace EventStore.Plugins.EventStoreReceiver.Config
{
    public class Receiver
    {
        public Receiver(string name, string id, string user, string password, string inputStream, bool appendInCaseOfConflict, int checkpointInterval)
        {
            Name = name;
            Id = id;
            User = user;
            Password = password;
            InputStream = inputStream;
            AppendInCaseOfConflict = appendInCaseOfConflict;
            CheckpointInterval = checkpointInterval;
        }

        public string Name { get; }
        public string Id { get; }
        public string User { get; }
        public string Password { get; }
        public string InputStream { get; }
        public bool AppendInCaseOfConflict { get; }
        public int CheckpointInterval { get; }

        public override string ToString()
        {
            return $"{Name}-{Id}";
        }
    }
}
