using System;
using Amqp.Framing;
using EventStore.Core.Services.Transport.Amqp;
using NUnit.Framework;
using Message = AmqpLite.Message;

namespace EventStore.Core.Tests.Services.Transport.Amqp
{
    [TestFixture]
    public class core_amqp_package
    {
        [Test]
        public void authorized_should_serialize_and_deserialize_correctly()
        {
            var corrId = Guid.NewGuid();
            var refPkg = new AmqpPackage(AmqpCommand.BadRequest, AmqpFlags.Authenticated, corrId, "login", "pa$$",
                new byte[] {1, 2, 3});
            var bytes = refPkg.AsArraySegment();

            var pkg = AmqpPackage.FromArraySegment(bytes);
            Assert.AreEqual(AmqpCommand.BadRequest, pkg.Command);
            Assert.AreEqual(AmqpFlags.Authenticated, pkg.Flags);
            Assert.AreEqual(corrId, pkg.CorrelationId);
            Assert.AreEqual("login", pkg.Login);
            Assert.AreEqual("pa$$", pkg.Password);

            Assert.AreEqual(0, pkg.Data.Count);
        }

        [Test]
        public void authorized_should_deserialize_amqp_WriteEvents_message_correctly()
        {
            const int reservedBytes = 40;
            var request = new Message("hello mate")
            {
                Properties =
                    new Properties() {MessageId = "command-request", ReplyTo = "client-" + Guid.NewGuid()},
                ApplicationProperties = new ApplicationProperties {["offset"] = 0}
            };
            var buffer = request.Encode(reservedBytes);

            var corrId = Guid.NewGuid();
            var refPkg = new AmqpPackage(AmqpCommand.WriteEvents, AmqpFlags.Authenticated, corrId, "login", "pa$$",
                buffer.Buffer);
            var bytes = refPkg.AsArraySegment();

            var pkg = AmqpPackage.FromArraySegment(bytes);
            Assert.AreEqual(AmqpCommand.WriteEvents, pkg.Command);
            Assert.AreEqual(AmqpFlags.Authenticated, pkg.Flags);
            Assert.AreEqual(corrId, pkg.CorrelationId);
            Assert.AreEqual("login", pkg.Login);
            Assert.AreEqual("pa$$", pkg.Password);
        }
    }
}
