using System.Collections.Generic;
using System.Threading;
using EventStore.Core.Messaging;

namespace EventStore.Core.Tests.AllReaderTests
{
    internal class InternalReplyEnvelope : IEnvelope
    {
        private readonly ManualResetEventSlim _signal;

        public List<Message> Replies = new List<Message>();

        public InternalReplyEnvelope(ManualResetEventSlim signal)
        {
            _signal = signal;
        }

        public void ReplyWith<T>(T message) where T : Message
        {
            Replies.Add(message);
            _signal.Set();
        }
    }
}