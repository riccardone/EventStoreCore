using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
//using Amqp;
//using Amqp.Framing;
//using Amqp.Types;
using Trace = System.Diagnostics.Trace;
using TraceLevel = System.Diagnostics.TraceLevel;

namespace EventStore.Core.Services.Transport.Amqp
{
    //class EventStoreServer : Example
    //{
    //    public override void Run()
    //    {
    //        string[] priorites = new string[] { "low", "medium", "high" };


    //        // receive messages from a specific session
    //        this.ReceiveMessages(10, priorites[2]);

    //        // receive messages from any available session
    //        this.ReceiveMessages(10, null);
    //    }

    //    void ReceiveMessages(int count, string sessionId)
    //    {
    //        var connection = new Connection(this.GetAddress());
    //        var session = new Session(connection);
    //        var filters = new Map {{new Symbol("com.microsoft:session-filter"), sessionId}};
    //        var receiver = new ReceiverLink(
    //            session,
    //            "sessionful-receiver-link",
    //            new Source() { Address = this.Entity, FilterSet = filters },
    //            null);

    //        for (var i = 0; i < count; i++)
    //        {
    //            var message = receiver.Receive(30000);
    //            if (message == null)
    //            {
    //                break;
    //            }
    //            receiver.Accept(message);
    //        }
    //        receiver.Close();
    //        session.Close();
    //        connection.Close();
    //    }
    //}
}
