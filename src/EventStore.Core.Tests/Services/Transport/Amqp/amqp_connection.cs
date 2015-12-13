using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using AmqpLite;
using EventStore.Common.Log;
using EventStore.Core.Services.Transport.Amqp.Lite;
using EventStore.Core.Services.Transport.Amqp.Lite.Serialization;
using EventStore.Core.Services.Transport.Tcp;
using EventStore.Core.Tests.Helpers;
using EventStore.Transport.Tcp;
using NUnit.Framework;
using List = NUnit.Framework.List;

namespace EventStore.Core.Tests.Services.Transport.Amqp
{
    [TestFixture, Category("LongRunning")]
    public class amqp_connection
    {
        [Test]
        public void should_connect_to_each_other_and_send_data()
        {
            var ip = IPAddress.Loopback;
            var port = PortsHelper.GetAvailablePort(ip);
            var serverEndPoint = new IPEndPoint(ip, port);

            const int reservedBytes = 40;
            var request = new Message("hello mate")
            {
                Properties =
                    new Properties() { MessageId = "command-request", ReplyTo = "client-" + Guid.NewGuid() },
                ApplicationProperties = new ApplicationProperties {["offset"] = 0 }
            };
            var buffer = request.Encode(reservedBytes);

            var bman = new BufferManager(256, 2*1024*1024, 100*1024*1024);
            //var ciccio = ProtobufExtensions.Serialize(request);

            var sent = new byte[1000];
            new Random().NextBytes(sent);

            var received = new MemoryStream();

            var done = new ManualResetEventSlim();

            var listener = new TcpServerListener(serverEndPoint);
            listener.StartListening((endPoint, socket) =>
            {
                var conn = AmqpConnection.CreateAcceptedAmqpConnection(Guid.NewGuid(), endPoint, socket, verbose: true);
                conn.ConnectionClosed += (x, y) => done.Set();
                if (conn.IsClosed) done.Set();
                
                Action<ITcpConnection, IEnumerable<ArraySegment<byte>>> callback = null;
                callback = (x, y) =>
                {
                    foreach (var arraySegment in y)
                    {
                        received.Write(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
                        //Log.Info("Received: {0} bytes, total: {1}.", arraySegment.Count, received.Length);
                    }

                    if (received.Length >= sent.Length)
                    {
                        done.Set();
                    }
                    else
                    {
                        conn.ReceiveAsync(callback);
                    }
                };
                conn.ReceiveAsync(callback);
            }, "Secure");

            var client = AmqpConnection.CreateConnectingAmqpConnection(
                Guid.NewGuid(), 
                serverEndPoint, 
                new TcpClientConnector(),
                TcpConnectionManager.ConnectionTimeout,
                conn =>
                {
                    conn.EnqueueSend(new[] {new ArraySegment<byte>(sent)});
                },
                (conn, err) =>
                {
                    done.Set();
                },
                verbose: true);

            Assert.IsTrue(done.Wait(20000), "Took too long to receive completion.");
            listener.Stop();
            client.Close("Normal close.");
            Assert.AreEqual(sent, received.ToArray());
        }

        public void AmqpSerializerListEncodingTest()
        {
            

            // Create an object to be serialized
            var p = new Student("Tom")
            {
                Address = new StreetAddress() { FullAddress = new string('B', 1024) },
                Grades = new List<int>() { 1, 2, 3, 4, 5 }
            };

            p.Age = 20;
            p.DateOfBirth = new DateTime(1980, 5, 12, 10, 2, 45, DateTimeKind.Utc);
            p.Properties.Add("height", 6.1);
            p.Properties.Add("male", true);
            p.Properties.Add("nick-name", "big foot");

            byte[] workBuffer = new byte[4096];
            ByteBuffer buffer = new ByteBuffer(workBuffer, 0, 0, workBuffer.Length);

            AmqpSerializer.Serialize(buffer, p);
            Assert.AreEqual(2, p.Version);

            // Deserialize and verify
            Person p3 = AmqpSerializer.Deserialize<Person>(buffer);
            Assert.AreEqual(2, p.Version);
            personValidator(p, p3);
            Assert.AreEqual(((Student)p).Address.FullAddress, ((Student)p3).Address.FullAddress);
            gradesValidator(((Student)p).Grades, ((Student)p3).Grades);

            // Inter-op: it should be an AMQP described list as other clients see it
            buffer.Seek(0);
            DescribedValue dl1 = AmqpSerializer.Deserialize<DescribedValue>(buffer);
            Assert.AreEqual(dl1.Descriptor, 0x0000123400000001UL);
            List lv = dl1.Value as List;
            Assert.IsTrue(lv != null);
            Assert.AreEqual(p.Name, lv[0]);
            Assert.AreEqual(p.Age, lv[1]);
            Assert.AreEqual(p.DateOfBirth.Value, lv[2]);
            Assert.IsTrue(lv[3] is DescribedValue, "Address is decribed type");
            Assert.AreEqual(((DescribedValue)lv[3]).Descriptor, 0x0000123400000003UL);
            Assert.AreEqual(((List)((DescribedValue)lv[3]).Value)[0], ((Student)p).Address.FullAddress);
            Assert.IsTrue(lv[4] is Map, "Properties should be map");
            Assert.AreEqual(((Map)lv[4])["height"], p.Properties["height"]);
            Assert.AreEqual(((Map)lv[4])["male"], p.Properties["male"]);
            Assert.AreEqual(((Map)lv[4])["nick-name"], p.Properties["nick-name"]);
            Assert.IsTrue(lv[5] is List);

            // Non-default serializer
            AmqpSerializer serializer = new AmqpSerializer();
            ByteBuffer bf1 = new ByteBuffer(1024, true);
            serializer.WriteObject(bf1, p);

            Person p4 = serializer.ReadObject<Person>(bf1);
            personValidator(p, p4);

            // Extensible: more items in the payload should not break
            DescribedValue dl2 = new DescribedValue(
                new Symbol("test.amqp:teacher"),
                new List() { "Jerry", 40, null, 50000, lv[4], null, null, "unknown-string", true, new Symbol("unknown-symbol") });
            ByteBuffer bf2 = new ByteBuffer(1024, true);
            serializer.WriteObject(bf2, dl2);
            serializer.WriteObject(bf2, 100ul);

            Person p5 = serializer.ReadObject<Person>(bf2);
            Assert.IsTrue(p5 is Teacher);
            Assert.IsTrue(p5.DateOfBirth == null);  // nullable should work
            Assert.AreEqual(100ul, serializer.ReadObject<object>(bf2));   // unknowns should be skipped
            Assert.AreEqual(0, bf2.Length);

            // teacher
            Teacher teacher = new Teacher("Han");
            teacher.Age = 30;
            teacher.Sallary = 60000;
            teacher.Classes = new Dictionary<int, string>() { { 101, "CS" }, { 102, "Math" }, { 205, "Project" } };

            ByteBuffer bf3 = new ByteBuffer(1024, true);
            serializer.WriteObject(bf3, teacher);

            Person p6 = serializer.ReadObject<Person>(bf3);
            Assert.IsTrue(p6 is Teacher);
            Assert.AreEqual(teacher.Age + 1, p6.Age);
            Assert.AreEqual(teacher.Sallary * 2, ((Teacher)p6).Sallary);
            Assert.AreEqual(teacher.Id, ((Teacher)p6).Id);
            Assert.AreEqual(teacher.Classes[101], ((Teacher)p6).Classes[101]);
            Assert.AreEqual(teacher.Classes[102], ((Teacher)p6).Classes[102]);
            Assert.AreEqual(teacher.Classes[205], ((Teacher)p6).Classes[205]);
        }
    }
}
