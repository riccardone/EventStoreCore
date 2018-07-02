using System;

namespace EventStore.Plugins.GeoReplica.Config
{
    public class Destination
    {
        public Destination(string name, string id, Uri connectionString, string inputStream, int interval, int batchSize, string user, string password)
        {
            Name = name;
            Id = id;
            ConnectionString = connectionString;
            InputStream = inputStream;
            Interval = interval;
            BatchSize = batchSize;
            User = user;
            Password = password;
        }

        public string Name { get; }
        public string Id { get; }
        public Uri ConnectionString { get; }
        public string InputStream { get; }
        public int Interval { get; }
        public int BatchSize { get; }
        public string User { get; }
        public string Password { get; }

        public override string ToString()
        {
            return $"{Name}-{Id}";
        }
    }
}
