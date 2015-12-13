using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Amqp;

namespace EventStore.Core.Services.Transport.Amqp
{
    abstract class Example
    {
        public string Namespace = "localhost";
        public string Entity = "myEntity";

        public Address GetAddress()
        {
            return new Address(this.Namespace, 5672, "admin", "changeit", "/", "ES");
        }

        public abstract void Run();
    }
}
