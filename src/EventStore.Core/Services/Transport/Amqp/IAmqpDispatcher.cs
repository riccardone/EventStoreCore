using System.Security.Principal;
using EventStore.Core.Messaging;
using EventStore.Core.Services.Transport.Tcp;

namespace EventStore.Core.Services.Transport.Amqp
{
    public interface IAmqpDispatcher
    {
        AmqpPackage? WrapMessage(Message message);
        Message UnwrapPackage(AmqpPackage package, IEnvelope envelope, IPrincipal user, string login, string pass, AmqpConnectionManager connection);
    }
}